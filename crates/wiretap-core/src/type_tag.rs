//! Parser for Sui Move type tags.
//!
//! Grammar (informal):
//!   tag       := primitive | struct
//!   primitive := "bool"|"u8"|"u16"|"u32"|"u64"|"u128"|"u256"|"address"|"signer"
//!              | "vector" "<" tag ">"
//!   struct    := address "::" ident "::" ident [ "<" tag ("," tag)* ">" ]
//!   address   := "0x" hex+
//!   ident     := [a-zA-Z_][a-zA-Z0-9_]*
//!
//! Whitespace between tokens is allowed and ignored. This is a small,
//! deterministic recursive-descent parser — there's no BCS in sight; we
//! only parse the textual tag and hand each tag off to
//! `LayoutResolver` for layout fetch + decode.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TypeTag {
    Bool,
    U8,
    U16,
    U32,
    U64,
    U128,
    U256,
    Address,
    Signer,
    Vector(Box<TypeTag>),
    Struct(StructTag),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StructTag {
    /// Hex-prefixed package id, normalized to lowercase, NOT zero-padded.
    pub package: String,
    pub module: String,
    pub name: String,
    pub type_params: Vec<TypeTag>,
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("unexpected end of tag at byte {0}")]
    Eof(usize),
    #[error("unexpected `{ch}` at byte {pos}")]
    Unexpected { ch: char, pos: usize },
    #[error("expected `{want}` at byte {pos}")]
    Expected { want: &'static str, pos: usize },
    #[error("identifier must start with letter or underscore at byte {0}")]
    BadIdent(usize),
}

impl fmt::Display for TypeTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypeTag::Bool => f.write_str("bool"),
            TypeTag::U8 => f.write_str("u8"),
            TypeTag::U16 => f.write_str("u16"),
            TypeTag::U32 => f.write_str("u32"),
            TypeTag::U64 => f.write_str("u64"),
            TypeTag::U128 => f.write_str("u128"),
            TypeTag::U256 => f.write_str("u256"),
            TypeTag::Address => f.write_str("address"),
            TypeTag::Signer => f.write_str("signer"),
            TypeTag::Vector(t) => write!(f, "vector<{}>", t),
            TypeTag::Struct(s) => fmt::Display::fmt(s, f),
        }
    }
}

impl fmt::Display for StructTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}::{}::{}", self.package, self.module, self.name)?;
        if !self.type_params.is_empty() {
            f.write_str("<")?;
            for (i, t) in self.type_params.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                fmt::Display::fmt(t, f)?;
            }
            f.write_str(">")?;
        }
        Ok(())
    }
}

impl TypeTag {
    pub fn parse(s: &str) -> Result<Self, ParseError> {
        let mut p = Parser { src: s, pos: 0 };
        p.skip_ws();
        let t = p.parse_tag()?;
        p.skip_ws();
        if p.pos != s.len() {
            return Err(ParseError::Unexpected {
                ch: s[p.pos..].chars().next().unwrap_or(' '),
                pos: p.pos,
            });
        }
        Ok(t)
    }
}

impl StructTag {
    pub fn parse(s: &str) -> Result<Self, ParseError> {
        match TypeTag::parse(s)? {
            TypeTag::Struct(s) => Ok(s),
            other => Err(ParseError::Unexpected {
                ch: other.to_string().chars().next().unwrap_or(' '),
                pos: 0,
            }),
        }
    }
}

struct Parser<'a> {
    src: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<char> {
        self.src[self.pos..].chars().next()
    }
    fn bump(&mut self, ch: char) {
        self.pos += ch.len_utf8();
    }
    fn skip_ws(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_whitespace() {
                self.bump(c);
            } else {
                break;
            }
        }
    }
    fn eat(&mut self, want: char) -> Result<(), ParseError> {
        self.skip_ws();
        match self.peek() {
            Some(c) if c == want => {
                self.bump(c);
                Ok(())
            }
            Some(c) => Err(ParseError::Unexpected { ch: c, pos: self.pos }),
            None => Err(ParseError::Eof(self.pos)),
        }
    }
    /// Try to eat `s` literally; on failure, do not advance.
    fn try_keyword(&mut self, kw: &'static str) -> bool {
        let rest = &self.src[self.pos..];
        let next = rest.as_bytes().get(kw.len()).copied();
        if rest.starts_with(kw)
            && next.map_or(true, |b| !is_ident_continue(b as char))
        {
            self.pos += kw.len();
            true
        } else {
            false
        }
    }
    fn parse_ident(&mut self) -> Result<&'a str, ParseError> {
        self.skip_ws();
        let start = self.pos;
        match self.peek() {
            Some(c) if c.is_alphabetic() || c == '_' => self.bump(c),
            _ => return Err(ParseError::BadIdent(self.pos)),
        }
        while let Some(c) = self.peek() {
            if is_ident_continue(c) {
                self.bump(c);
            } else {
                break;
            }
        }
        Ok(&self.src[start..self.pos])
    }
    fn parse_address(&mut self) -> Result<String, ParseError> {
        self.skip_ws();
        if !self.src[self.pos..].starts_with("0x")
            && !self.src[self.pos..].starts_with("0X")
        {
            return Err(ParseError::Expected {
                want: "0x-prefixed address",
                pos: self.pos,
            });
        }
        let start = self.pos;
        self.pos += 2;
        let hex_start = self.pos;
        while let Some(c) = self.peek() {
            if c.is_ascii_hexdigit() {
                self.bump(c);
            } else {
                break;
            }
        }
        if self.pos == hex_start {
            return Err(ParseError::Expected {
                want: "hex digits after 0x",
                pos: self.pos,
            });
        }
        Ok(self.src[start..self.pos].to_ascii_lowercase())
    }
    fn parse_tag(&mut self) -> Result<TypeTag, ParseError> {
        self.skip_ws();
        if self.try_keyword("bool") {
            return Ok(TypeTag::Bool);
        }
        if self.try_keyword("address") {
            return Ok(TypeTag::Address);
        }
        if self.try_keyword("signer") {
            return Ok(TypeTag::Signer);
        }
        for (kw, t) in [
            ("u128", TypeTag::U128),
            ("u256", TypeTag::U256),
            ("u64", TypeTag::U64),
            ("u32", TypeTag::U32),
            ("u16", TypeTag::U16),
            ("u8", TypeTag::U8),
        ] {
            if self.try_keyword(kw) {
                return Ok(t);
            }
        }
        if self.try_keyword("vector") {
            self.eat('<')?;
            let inner = self.parse_tag()?;
            self.skip_ws();
            self.eat('>')?;
            return Ok(TypeTag::Vector(Box::new(inner)));
        }
        // Struct tag.
        let address = self.parse_address()?;
        self.skip_ws();
        self.eat(':')?;
        self.eat(':')?;
        let module = self.parse_ident()?.to_string();
        self.skip_ws();
        self.eat(':')?;
        self.eat(':')?;
        let name = self.parse_ident()?.to_string();
        self.skip_ws();
        let type_params = if self.peek() == Some('<') {
            self.bump('<');
            let mut out = Vec::new();
            loop {
                self.skip_ws();
                out.push(self.parse_tag()?);
                self.skip_ws();
                match self.peek() {
                    Some(',') => {
                        self.bump(',');
                    }
                    Some('>') => {
                        self.bump('>');
                        break;
                    }
                    Some(c) => return Err(ParseError::Unexpected { ch: c, pos: self.pos }),
                    None => return Err(ParseError::Eof(self.pos)),
                }
            }
            out
        } else {
            Vec::new()
        };
        Ok(TypeTag::Struct(StructTag {
            package: address,
            module,
            name,
            type_params,
        }))
    }
}

fn is_ident_continue(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primitives() {
        assert_eq!(TypeTag::parse("u8").unwrap(), TypeTag::U8);
        assert_eq!(TypeTag::parse("bool").unwrap(), TypeTag::Bool);
        assert_eq!(TypeTag::parse("address").unwrap(), TypeTag::Address);
    }

    #[test]
    fn vector_nested() {
        let t = TypeTag::parse("vector<vector<u8>>").unwrap();
        assert_eq!(t.to_string(), "vector<vector<u8>>");
    }

    #[test]
    fn struct_no_generics() {
        let t = TypeTag::parse("0x2::sui::SUI").unwrap();
        let TypeTag::Struct(s) = t else { panic!() };
        assert_eq!(s.package, "0x2");
        assert_eq!(s.module, "sui");
        assert_eq!(s.name, "SUI");
        assert!(s.type_params.is_empty());
    }

    #[test]
    fn struct_with_generics() {
        let t = TypeTag::parse("0xabc::pool::SwapEvent<0x2::sui::SUI, 0xdef::usdc::USDC>")
            .unwrap();
        let TypeTag::Struct(s) = t else { panic!() };
        assert_eq!(s.name, "SwapEvent");
        assert_eq!(s.type_params.len(), 2);
    }

    #[test]
    fn nested_generics() {
        let t = TypeTag::parse(
            "0xabc::vault::Balance<0x2::coin::Coin<0x2::sui::SUI>>",
        )
        .unwrap();
        assert_eq!(
            t.to_string(),
            "0xabc::vault::Balance<0x2::coin::Coin<0x2::sui::SUI>>"
        );
    }

    #[test]
    fn rejects_garbage() {
        assert!(TypeTag::parse("not a tag").is_err());
        assert!(TypeTag::parse("0x2::sui::").is_err());
        assert!(TypeTag::parse("0x2::sui::SUI<").is_err());
    }
}
