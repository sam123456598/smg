//! The formats: how each model family writes reasoning and tool calls, each a [`Format`] table
//! the [`Engine`] runs.
//!
//! [`qwen3()`] is the first: `<think>` blocks and `<tool_call>` blocks, each holding one call in
//! the family's call syntax, a JSON object (Qwen3) or tags (Qwen 3.5 and later, Qwen3-Coder).
//! [`qwen2_5()`] is the same family before thinking: `<tool_call>` blocks alone, and `<think>` is
//! text.
//!
//! [`Format`]: crate::Format
//! [`Engine`]: crate::Engine

pub mod qwen2_5;
pub mod qwen3;

pub use qwen2_5::qwen2_5;
pub use qwen3::qwen3;
