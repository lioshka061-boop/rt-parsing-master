use crate::product_category::ProductCategory;
use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

static DEFAULT_REGEX: Lazy<HashMap<&'static str, Regex>> = Lazy::new(|| {
    let pairs: Vec<(&str, &str)> = vec![
        (
            "спліттери",
            r"(?i)(спліттер|сплиттер|splitter|lip|губа|передній дифузор|передный диффузор)",
        ),
        (
            "дифузори",
            r"(?i)(дифузор|диффузор|diffuser|задній дифузор|задний диффузор)",
        ),
        ("спойлери", r"(?i)(спойлер|spoiler)"),
        ("пороги", r"(?i)(поріг|порог|side\s*skirt|skirt|пороги)"),
        (
            "решітки-радіатора",
            r"(?i)(решітка|решетка|решітки|решетки|grill|grille|гриль)",
        ),
        (
            "решітки",
            r"(?i)(решітка|решетка|решітки|решетки|grill|grille|гриль)",
        ),
        ("бампери", r"(?i)(бампер|bumper|бампери)"),
        ("диски", r"(?i)(диск|диски|wheels?|r\\d{2}\\s|r\\d{2}\\b)"),
    ];
    pairs
        .into_iter()
        .filter_map(|(name, re)| Regex::new(re).ok().map(|r| (name, r)))
        .collect()
});

const HAYSTACK_DESC_LIMIT: usize = 800;

fn truncate_chars(input: &str, max: usize) -> &str {
    if input.is_empty() || input.len() <= max {
        return input;
    }
    let mut end = 0usize;
    let mut count = 0usize;
    for (idx, ch) in input.char_indices() {
        if count >= max {
            break;
        }
        end = idx + ch.len_utf8();
        count += 1;
    }
    &input[..end]
}

pub fn build_haystack(title: &str, description: &str) -> String {
    let desc = truncate_chars(description.trim(), HAYSTACK_DESC_LIMIT);
    let mut out = String::with_capacity(title.len() + 1 + desc.len());
    out.push_str(title);
    if !desc.is_empty() {
        out.push('\n');
        out.push_str(desc);
    }
    out
}

fn normalize_text(input: &str) -> String {
    input
        .to_lowercase()
        .replace(['_', '/', '\\', '—', '-', '–'], " ")
        .replace(|c: char| !c.is_alphanumeric() && !c.is_whitespace(), " ")
}

fn tokens_from_category_name(name: &str) -> Vec<String> {
    let stop: HashSet<&'static str> = [
        "і",
        "й",
        "та",
        "або",
        "для",
        "в",
        "у",
        "на",
        "по",
        "з",
        "до",
        "від",
        "під",
        "над",
        "комплект",
        "набір",
        "набори",
        "комплекти",
    ]
    .into_iter()
    .collect();

    normalize_text(name)
        .split_whitespace()
        .map(|t| t.trim())
        .filter(|t| t.len() >= 3)
        .filter(|t| !stop.contains(*t))
        .flat_map(|t| {
            let mut v = vec![t.to_string()];
            // Проста "сингуларизація": обрізаємо типові закінчення множини
            for suffix in ["и", "і", "ї", "ы", "я", "а", "s", "es"] {
                if t.ends_with(suffix) && t.len() > suffix.len() + 2 {
                    v.push(t.trim_end_matches(suffix).to_string());
                }
            }
            v
        })
        .collect()
}

fn depth_of(mut id: Uuid, by_id: &HashMap<Uuid, &ProductCategory>) -> usize {
    let mut depth = 0usize;
    let mut backtrace = HashSet::<Uuid>::new();
    while let Some(parent) = by_id.get(&id).and_then(|c| c.parent_id) {
        if !backtrace.insert(parent) {
            break;
        }
        depth += 1;
        id = parent;
        if depth > 32 {
            break;
        }
    }
    depth
}

#[derive(Clone)]
pub struct PreparedCategory {
    id: Uuid,
    regex: Option<Regex>,
    tokens: Vec<String>,
    depth: usize,
    name_len: usize,
}

pub struct CategoryMatcher {
    ordered: Vec<PreparedCategory>,
}

impl CategoryMatcher {
    pub fn new(categories: &[ProductCategory]) -> Self {
        if categories.is_empty() {
            return Self { ordered: Vec::new() };
        }
        let by_id: HashMap<Uuid, &ProductCategory> = categories.iter().map(|c| (c.id, c)).collect();
        let mut ordered = categories
            .iter()
            .map(|c| {
                let slug_name = normalize_text(&c.name).replace(' ', "-");
                let regex = c
                    .regex
                    .as_ref()
                    .cloned()
                    .or_else(|| DEFAULT_REGEX.get(slug_name.as_str()).cloned());
                let tokens = if regex.is_some() {
                    Vec::new()
                } else {
                    tokens_from_category_name(&c.name)
                };
                PreparedCategory {
                    id: c.id,
                    regex,
                    tokens,
                    depth: depth_of(c.id, &by_id),
                    name_len: c.name.len(),
                }
            })
            .collect::<Vec<_>>();
        ordered.sort_by_key(|c| (std::cmp::Reverse(c.depth), std::cmp::Reverse(c.name_len)));
        Self { ordered }
    }

    pub fn guess(&self, haystack: &str) -> Option<Uuid> {
        if self.ordered.is_empty() {
            return None;
        }
        let haystack = haystack.trim();
        if haystack.is_empty() {
            return None;
        }
        let mut normalized: Option<String> = None;
        for c in &self.ordered {
            if let Some(re) = c.regex.as_ref() {
                if re.is_match(haystack) {
                    return Some(c.id);
                }
                continue;
            }
            if c.tokens.is_empty() {
                continue;
            }
            let hay = normalized.get_or_insert_with(|| normalize_text(haystack));
            if c.tokens.iter().any(|t| hay.contains(t)) {
                return Some(c.id);
            }
        }
        None
    }
}

pub fn guess_product_category_id(haystack: &str, categories: &[ProductCategory]) -> Option<Uuid> {
    CategoryMatcher::new(categories).guess(haystack)
}
