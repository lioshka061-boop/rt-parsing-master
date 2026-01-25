use rt_types::category::Category;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

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
        .filter(|t| t.len() >= 3 || (t.len() >= 2 && t.chars().any(|c| c.is_ascii_digit())))
        .filter(|t| !stop.contains(*t))
        .map(str::to_string)
        .collect()
}

fn depth_of(mut id: Uuid, by_id: &HashMap<Uuid, &Category>) -> usize {
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

pub fn guess_site_category_id(haystack: &str, categories: &[Category]) -> Option<Uuid> {
    if categories.is_empty() {
        return None;
    }

    let haystack = haystack.trim();
    if haystack.is_empty() {
        return None;
    }

    let by_id: HashMap<Uuid, &Category> = categories.iter().map(|c| (c.id, c)).collect();
    let normalized = normalize_text(haystack);
    if normalized.trim().is_empty() {
        return None;
    }

    let mut best: Option<(usize, usize, usize, Uuid)> = None;
    for c in categories.iter() {
        if let Some(re) = c.regex.as_ref() {
            if re.is_match(haystack) {
                return Some(c.id);
            }
        }

        let tokens = tokens_from_category_name(&c.name);
        if tokens.is_empty() {
            continue;
        }

        let mut word_tokens = 0usize;
        let mut code_tokens = 0usize;
        let mut word_matches = 0usize;
        let mut code_matches = 0usize;
        for token in tokens {
            if token.chars().all(|c| c.is_ascii_digit()) {
                continue;
            }
            let is_code = token.chars().any(|c| c.is_ascii_digit());
            if is_code {
                code_tokens += 1;
            } else {
                word_tokens += 1;
            }
            if normalized.contains(&token) {
                if is_code {
                    code_matches += 1;
                } else {
                    word_matches += 1;
                }
            }
        }

        if code_tokens > 0 && code_matches == 0 {
            continue;
        }
        if word_tokens > 0 {
            let required_words = if word_tokens >= 2 { 2 } else { 1 };
            if word_matches < required_words {
                continue;
            }
        }

        let depth = depth_of(c.id, &by_id);
        let score = code_matches * 4 + word_matches * 2;
        let rank = (score, depth, c.name.len());
        let should_replace = match best {
            None => true,
            Some(prev) => rank > (prev.0, prev.1, prev.2),
        };
        if should_replace {
            best = Some((rank.0, rank.1, rank.2, c.id));
        }
    }

    best.map(|(_, _, _, id)| id)
}

fn root_ancestor<'a>(start: &'a Category, by_id: &HashMap<Uuid, &'a Category>) -> &'a Category {
    let mut cur = start;
    let mut backtrace = HashSet::<Uuid>::new();
    while let Some(parent_id) = cur.parent_id {
        if !backtrace.insert(parent_id) {
            break;
        }
        if let Some(p) = by_id.get(&parent_id) {
            cur = p;
        } else {
            break;
        }
    }
    cur
}

fn is_descendant_of<'a>(
    mut candidate: &'a Category,
    root_id: Uuid,
    by_id: &HashMap<Uuid, &'a Category>,
) -> bool {
    let mut backtrace = HashSet::<Uuid>::new();
    loop {
        if candidate.id == root_id {
            return true;
        }
        if let Some(parent_id) = candidate.parent_id {
            if !backtrace.insert(parent_id) {
                return false;
            }
            if let Some(parent) = by_id.get(&parent_id) {
                candidate = parent;
                continue;
            }
        }
        return false;
    }
}

fn detect_brand<'a>(
    haystack: &str,
    normalized: &str,
    categories: &'a [Category],
) -> Option<&'a Category> {
    let mut best: Option<&Category> = None;
    for c in categories.iter().filter(|c| c.parent_id.is_none()) {
        if let Some(re) = c.regex.as_ref() {
            if re.is_match(haystack) {
                return Some(c);
            }
        }
        let tokens = tokens_from_category_name(&c.name);
        if tokens.is_empty() {
            continue;
        }
        if tokens.iter().all(|t| normalized.contains(t)) {
            let replace = match best {
                None => true,
                Some(prev) => c.name.len() > prev.name.len(),
            };
            if replace {
                best = Some(c);
            }
        }
    }
    best
}

pub fn guess_brand_model(
    title: &str,
    description: Option<&str>,
    categories: &[Category],
) -> Option<(String, String, Option<Uuid>)> {
    if categories.is_empty() {
        return None;
    }

    let mut haystack = String::new();
    if !title.trim().is_empty() {
        haystack.push_str(title.trim());
    }
    if let Some(desc) = description {
        let desc = desc.trim();
        if !desc.is_empty() {
            if !haystack.is_empty() {
                haystack.push(' ');
            }
            haystack.push_str(desc);
        }
    }

    let normalized = normalize_text(&haystack);
    if normalized.trim().is_empty() {
        return None;
    }

    let by_id: HashMap<Uuid, &Category> = categories.iter().map(|c| (c.id, c)).collect();
    let brand = detect_brand(&haystack, &normalized, categories);
    let filtered = if let Some(brand) = brand {
        categories
            .iter()
            .filter(|c| is_descendant_of(c, brand.id, &by_id) && c.id != brand.id)
            .cloned()
            .collect::<Vec<_>>()
    } else {
        categories.to_vec()
    };

    if let Some(id) = guess_site_category_id(&haystack, &filtered) {
        if let Some(category) = by_id.get(&id) {
            let brand = root_ancestor(category, &by_id);
            return Some((brand.name.clone(), category.name.clone(), Some(id)));
        }
    }

    brand.map(|b| (b.name.clone(), b.name.clone(), None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn category(name: &str, id: Uuid, parent_id: Option<Uuid>, shop_id: Uuid) -> Category {
        Category {
            name: name.to_string(),
            id,
            parent_id,
            regex: None,
            shop_id,
            seo_title: None,
            seo_description: None,
            seo_text: None,
        }
    }

    #[test]
    fn guesses_brand_and_model_from_codes() {
        let shop_id = Uuid::new_v4();
        let brand_id = Uuid::new_v4();
        let model_id = Uuid::new_v4();
        let brand = category("BMW", brand_id, None, shop_id);
        let model = category("BMW X5 G05 (2019-...)", model_id, Some(brand_id), shop_id);
        let categories = vec![brand.clone(), model.clone()];

        let guess = guess_brand_model("Спойлер BMW X5 G05", None, &categories).unwrap();
        assert_eq!(guess.0, "BMW");
        assert_eq!(guess.1, model.name);
        assert_eq!(guess.2, Some(model_id));
    }
}
