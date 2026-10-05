//! Which tokens wrote which bytes, so that every text event can say how many tokens it carries.
//!
//! The engine hands each delta with the spans of its tokens ([`TokenSpan`]). A format emits the
//! output's bytes in order, each byte in exactly one event (the conservation property), so the
//! [`Ledger`] needs no map from bytes to events: it keeps the absolute offsets at which tokens start
//! and, as the format emits each run of text, counts the tokens that start inside it. A token is
//! counted in the event that carries its first byte; a span continued from an earlier delta is not
//! a new token; a token with no bytes is a control token and is reported as `Dropped` with
//! `ControlToken`, a count of one and no text, at the point of the stream where it arrived. The sum
//! over all events is then the number of tokens: token identity, the design's fifth property.
//!
//! Counting is all or nothing per stream: a delta without spans for non-empty text, or with spans
//! that do not partition its text, turns the rest of the stream uncounted (`tokens: None`), since a
//! partial count would be a wrong one.

use std::collections::VecDeque;

use crate::{
    event::{DropReason, Event, Events, Text},
    input::TokenSpan,
};

/// The tokens of one output, counted into the events that carry their first byte.
#[derive(Clone, Debug, Default)]
pub struct Ledger {
    /// Absolute offsets at which tokens not yet counted start, in order.
    starts: VecDeque<usize>,
    /// Bytes of output received so far.
    received: usize,
    /// Bytes of output emitted so far.
    emitted: usize,
    /// Whether the stream is still counted.
    uncounted: bool,
}

impl Ledger {
    /// A ledger for a new output.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether the texts this ledger hands out carry counts.
    pub fn counting(&self) -> bool {
        !self.uncounted
    }

    /// Take note of a delta's tokens before its text is parsed; control tokens are reported now.
    pub fn note(&mut self, text: &str, spans: &[TokenSpan], out: &mut Events) {
        let at = self.received;
        self.received += text.len();
        if self.uncounted {
            return;
        }
        if (spans.is_empty() && !text.is_empty()) || !TokenSpan::partitions(text, spans) {
            self.uncounted = true;
            self.starts.clear();
            return;
        }
        for span in spans {
            if span.continued {
                continue;
            }
            if span.start == span.end {
                out.push(Event::Dropped {
                    text: Text::new("", 1),
                    why: DropReason::ControlToken,
                });
            } else {
                self.starts.push_back(at + span.start);
            }
        }
    }

    /// The next run of the output's bytes as text, with the tokens that start in it.
    pub fn text(&mut self, bytes: &str) -> Text {
        let end = self.emitted + bytes.len();
        self.emitted = end;
        if self.uncounted {
            return Text::uncounted(bytes);
        }
        let mut count = 0;
        while self.starts.front().is_some_and(|&start| start < end) {
            self.starts.pop_front();
            count += 1;
        }
        Text::new(bytes, count)
    }

    /// The same event with its text counted, for events another component built uncounted.
    pub fn relabel(&mut self, event: Event) -> Event {
        match event {
            Event::Content(t) => Event::Content(self.text(&t.text)),
            Event::Reasoning(t) => Event::Reasoning(self.text(&t.text)),
            Event::Dropped { text, why } => Event::Dropped {
                text: self.text(&text.text),
                why,
            },
            Event::Malformed { text, why } => Event::Malformed {
                text: self.text(&text.text),
                why,
            },
            Event::ToolCallStart {
                index,
                id,
                name,
                source,
            } => Event::ToolCallStart {
                index,
                id,
                name,
                source: self.text(&source.text),
            },
            Event::ToolCallArguments {
                index,
                json,
                source,
            } => Event::ToolCallArguments {
                index,
                json,
                source: self.text(&source.text),
            },
            Event::ToolCallEnd { index, source } => Event::ToolCallEnd {
                index,
                source: self.text(&source.text),
            },
            other @ (Event::ReasoningStart | Event::ReasoningEnd | Event::Finish { .. }) => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span(start: usize, end: usize, continued: bool) -> TokenSpan {
        TokenSpan {
            token_id: 0,
            start,
            end,
            continued,
        }
    }

    #[test]
    fn a_token_is_counted_in_the_text_that_carries_its_first_byte() {
        let mut ledger = Ledger::new();
        let mut out = Events::new();
        // Tokens "ab", "cd", "e" over the text "abcde", emitted as "a" | "bcd" | "e".
        ledger.note(
            "abcde",
            &[span(0, 2, false), span(2, 4, false), span(4, 5, false)],
            &mut out,
        );
        assert_eq!(ledger.text("a"), Text::new("a", 1));
        assert_eq!(ledger.text("bcd"), Text::new("bcd", 1));
        assert_eq!(ledger.text("e"), Text::new("e", 1));
        assert!(out.is_empty());
    }

    #[test]
    fn a_span_continued_from_an_earlier_delta_is_not_a_new_token() {
        let mut ledger = Ledger::new();
        let mut out = Events::new();
        ledger.note("ab", &[span(0, 2, false)], &mut out);
        ledger.note("cd", &[span(0, 1, true), span(1, 2, false)], &mut out);
        assert_eq!(ledger.text("abc"), Text::new("abc", 1));
        assert_eq!(ledger.text("d"), Text::new("d", 1));
    }

    #[test]
    fn a_token_without_bytes_is_a_control_token_reported_at_once() {
        let mut ledger = Ledger::new();
        let mut out = Events::new();
        ledger.note("x", &[span(0, 0, false), span(0, 1, false)], &mut out);
        assert_eq!(
            out.drain(),
            vec![Event::Dropped {
                text: Text::new("", 1),
                why: DropReason::ControlToken,
            }]
        );
        assert_eq!(ledger.text("x"), Text::new("x", 1));
    }

    #[test]
    fn a_delta_without_spans_or_with_spans_that_do_not_fit_makes_the_rest_uncounted() {
        let mut ledger = Ledger::new();
        let mut out = Events::new();
        ledger.note("ab", &[span(0, 2, false)], &mut out);
        assert!(ledger.counting());
        ledger.note("cd", &[], &mut out);
        assert!(!ledger.counting());
        assert_eq!(ledger.text("abcd"), Text::uncounted("abcd"));
        let mut ledger = Ledger::new();
        ledger.note("ab", &[span(0, 1, false)], &mut out);
        assert!(
            !ledger.counting(),
            "spans that stop short of the text do not fit"
        );
        let mut ledger = Ledger::new();
        ledger.note("", &[], &mut out);
        assert!(ledger.counting(), "an empty delta needs no spans");
    }

    #[test]
    fn relabel_counts_an_events_text_in_order() {
        let mut ledger = Ledger::new();
        let mut out = Events::new();
        ledger.note("{}x", &[span(0, 2, false), span(2, 3, false)], &mut out);
        let start = ledger.relabel(Event::ToolCallStart {
            index: 0,
            id: "call_0".into(),
            name: "f".into(),
            source: Text::uncounted("{}"),
        });
        assert!(
            matches!(start, Event::ToolCallStart { source, .. } if source == Text::new("{}", 1))
        );
        assert_eq!(ledger.relabel(Event::ReasoningEnd), Event::ReasoningEnd);
        assert_eq!(ledger.text("x"), Text::new("x", 1));
    }
}
