use strum_macros::{Display, EnumIter, EnumString};

#[derive(Clone, Copy, Display, EnumIter, EnumString)]
pub enum RenameRule {
    #[strum(serialize = "camelCase")]
    Camel,
    #[strum(serialize = "kebab-case")]
    Kebab,
    #[strum(serialize = "lowercase")]
    Lower,
    #[strum(serialize = "PascalCase")]
    Pascal,
    #[strum(serialize = "SCREAMING-KEBAB-CASE")]
    ScreamingKebab,
    #[strum(serialize = "SCREAMING_SNAKE_CASE")]
    ScreamingSnake,
    #[strum(serialize = "snake_case")]
    Snake,
    #[strum(serialize = "UPPERCASE")]
    Upper,
}

impl From<RenameRule> for convert_case::Case {
    fn from(rule: RenameRule) -> Self {
        match rule {
            RenameRule::Camel => Self::Camel,
            RenameRule::Kebab => Self::Kebab,
            RenameRule::Lower => Self::Lower,
            RenameRule::Pascal => Self::Pascal,
            RenameRule::ScreamingKebab => Self::UpperKebab,
            RenameRule::ScreamingSnake => Self::UpperSnake,
            RenameRule::Snake => Self::Snake,
            RenameRule::Upper => Self::Upper,
        }
    }
}
