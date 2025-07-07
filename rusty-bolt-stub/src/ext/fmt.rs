use std::cell::RefCell;
use std::fmt::{Display, Formatter};

pub(crate) trait IterFmtExt {
    /// # Panics
    /// The returned Display implementation may panic if its `Display::fmt` method is called
    /// more than once.
    fn join_display(self, join: &'static str) -> impl Display;
}

impl<D: Display, I: Iterator<Item = D>> IterFmtExt for I {
    fn join_display(self, join: &'static str) -> impl Display {
        struct IterDisplay<I> {
            iter: RefCell<Option<I>>,
            join: &'static str,
        }

        impl<D: Display, I: Iterator<Item = D>> Display for IterDisplay<I> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                let mut iter = self
                    .iter
                    .borrow_mut()
                    .take()
                    .expect("join_display used more than once");
                if let Some(e) = iter.next() {
                    Display::fmt(&e, f)?;
                    for e in iter {
                        f.write_str(self.join)?;
                        Display::fmt(&e, f)?;
                    }
                }
                Ok(())
            }
        }

        IterDisplay {
            iter: RefCell::new(Some(self)),
            join,
        }
    }
}
