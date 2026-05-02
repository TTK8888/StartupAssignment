from __future__ import annotations

import re
from urllib.parse import urlparse

from ..settings import COUNTRY_TERMS, FUNDING_PATTERN, ROUND_PATTERNS


def country_from_text(*values: str) -> str:
    """Infer the target country or region from one or more text values."""
    for value in values:
        blob = value.lower()
        for country, terms in COUNTRY_TERMS.items():
            if any(term in blob for term in terms):
                return country
        if re.search(r"\b(southeast asia|south-east asia)\b", blob):
            return "Southeast Asia"
    return "Unknown"


def round_from_text(text: str) -> str:
    """Infer the investment round label from article text."""
    for pattern, label in ROUND_PATTERNS:
        if pattern.search(text):
            return label
    if re.search(r"\b(funding round|financing round|investment round)\b", text, re.IGNORECASE):
        return "Funding Round"
    if FUNDING_PATTERN.search(text):
        return "Funding Round"
    return "Not Available"


def lead_investor_from_text(text: str) -> str:
    """Extract the lead investor from common led-by funding phrases."""
    patterns = [
        r"\bco-led by\s+([^.;:]{2,120}?)(?:,?\s+(?:with|alongside|and participation|participating|joins portfolio|portfolio news|written by|share|read more)|[.;])",
        r"\bled by\s+([^.;:]{2,120}?)(?:,?\s+(?:with|alongside|and participation|participating|joins portfolio|portfolio news|written by|share|read more)|[.;])",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            candidate = clean_entity_list(match.group(1))
            if 2 <= len(candidate) <= 200 and not _looks_like_prose(candidate):
                return candidate
    return "Not Stated"


def investors_from_text(text: str, lead_investor: str) -> str:
    """Extract investor names from funding text, preferring a known lead investor."""
    # reuses a clear lead investor before trying looser investor patterns
    if lead_investor != "Not Stated":
        return lead_investor
    patterns = [
        r"\b([A-Z][\w'’&.-]+(?:\s+[A-Z][\w'’&.-]+){0,4})\s+backs\s+[^.;:]{2,120}?(?:[.;]|\s+to\s+)",
        r"\b(?:raised|secured|received|closed|raise|raises)\b[^.;:]{0,80}?\bfrom\s+([^.;:]{2,120}?)(?:[.;]|\s+to\s+)",
        r"\b(?:funding|investment|round|capital)\s+from\s+([^.;:]{2,120}?)(?:[.;]|\s+to\s+)",
        r"\bbacked by\s+([^.;:]{2,120}?)(?:[.;])",
        r"\bparticipation from\s+([^.;:]{2,120}?)(?:[.;])",
        r"\b(?:investors|backers)\s+include[ds]?\s+([^.;:]{2,120}?)(?:[.;])",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            candidate = clean_entity_list(match.group(1))
            if 2 <= len(candidate) <= 200 and not _looks_like_prose(candidate):
                return candidate
    return "Not Available"


def _looks_like_prose(value: str) -> bool:
    # rejects sentence fragments that were captured as entity lists
    lowered = f" {value.lower()} "
    prose_markers = [
        " is ", " was ", " are ", " were ", " has ", " have ",
        " will ", " would ", " should ", " can ", " may ", " might ",
        " which ", " that ", " help ", " helps ", " expand ",
        " catalyze ", " scale ", " grow ", " their ", " its ", " on ",
    ]
    if any(marker in lowered for marker in prose_markers):
        return True
    return False


def clean_entity_list(value: str) -> str:
    """Normalize captured entity text into a semicolon-separated name list."""
    value = re.split(
        r"\b(?:joins portfolio|portfolio news|written by|share|read more)\b",
        value,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    value = re.sub(r"\s+(?:and|alongside)\s+", "; ", value.strip(), flags=re.IGNORECASE)
    value = value.replace("&", ";")
    value = re.sub(r",\s*", "; ", value)
    value = re.sub(r"\s{2,}", " ", value)
    return value.strip(" ;,-")


def investor_type_from_names(investor_names: str) -> str:
    """Classify investor names into a coarse investor type label."""
    if investor_names in {"", "Not Available"}:
        return "Not Available"
    lowered = investor_names.lower()
    if any(term in lowered for term in ["angel", "angels"]):
        return "Angel"
    if any(term in lowered for term in ["accelerator", "incubator"]):
        return "Accelerator"
    if any(term in lowered for term in ["family office", "family-office"]):
        return "Family Office"
    if any(term in lowered for term in ["private equity", "buyout", " pe "]):
        return "PE"
    if any(term in lowered for term in ["government", "startup bangladesh", "sovereign"]):
        return "Government VC"
    if any(term in lowered for term in ["corporate", "bank", "telco", "group"]):
        return "Corporate VC"
    if any(term in lowered for term in ["ventures", "venture", "capital", "vc"]):
        return "VC"
    return "Not Available"


_INVESTOR_DEAL_VERBS = {
    "raises", "raised", "secures", "secured", "lands", "landed",
    "bags", "bagged", "closes", "closed", "wraps", "wrapped",
    "grabs", "grabbed", "hauls", "hauled", "attracts", "attracted",
    "pockets", "pocketed", "scores", "scored", "banks", "banked",
    "backs", "backed", "invests", "leads", "led", "gets", "got",
    "nets", "netted", "snags", "snagged", "nabs", "nabbed", "rakes",
    "raked", "completes", "completed", "announces", "announced",
    "receives", "received", "brings", "brought",
}

_DEAL_VERB_ALT = "|".join(sorted(_INVESTOR_DEAL_VERBS))
_DEAL_VERB_PATTERN = re.compile(rf"\b(?:{_DEAL_VERB_ALT})\b", re.IGNORECASE)

# deal verbs where the startup is the subject of the title
_TITLE_SUBJECT_VERBS = _INVESTOR_DEAL_VERBS - {
    "backs", "backed", "invests", "leads", "led",
}
_TITLE_SUBJECT_VERB_ALT = "|".join(sorted(_TITLE_SUBJECT_VERBS))


_SLUG_NOISE_TOKENS = {
    "startup", "startups", "fintech", "edtech", "healthtech", "deeptech",
    "foodtech", "agritech", "proptech", "climatetech", "biotech", "insurtech",
    "health", "tech", "deep", "food", "agri", "prop", "climate", "bio", "insur",
    "ai", "ml",
    "portfolio", "news", "announcement", "company", "firm", "business",
    "platform", "app", "marketplace", "player", "venture", "ventures",
    "based", "backed", "backs", "led",
    "the", "a", "an", "is", "are", "was", "were", "will",
    "has", "have", "had",
    "bangladesh", "bangladeshi", "bangladeshs",
    "singapore", "singaporean", "singapores", "singaporeans",
    "indonesia", "indonesian", "indonesias",
    "malaysia", "malaysian", "malaysias",
    "vietnam", "vietnamese", "vietnams",
    "philippines", "filipino", "philippine",
    "thailand", "thai", "thailands",
    "cambodia", "cambodian",
    "laos", "myanmar",
    "southeast", "south", "asia",
}

_SLUG_PREPOSITION_TOKENS = {"in", "to", "by", "on", "into", "from", "with"}


def _is_slug_noise(token: str) -> bool:
    if not token:
        return True
    lower = token.lower()
    if lower in _SLUG_NOISE_TOKENS:
        return True
    # url encoded native script cannot become a clean latin name
    if "%" in lower:
        return True
    if re.fullmatch(r"\d+(?:[.,]\d+)?[a-z]*", lower):
        return True
    return False


def _name_from_tokens(tokens: list[str]) -> str:
    if not tokens:
        return ""
    name = " ".join(t.capitalize() for t in tokens)
    cleaned = clean_startup_name(name)
    return cleaned if cleaned and not _is_generic_startup_name(cleaned) else ""


_BODY_NAME_NOISE = {
    "bangladesh", "singapore", "indonesia", "vietnam", "philippines",
    "malaysia", "thailand", "cambodia", "laos", "myanmar",
    "southeast asia", "south asia", "asia", "south", "southeast",
    "the company", "the firm", "the startup", "the business",
    "tech in asia", "deal street asia", "the daily star", "the business standard",
    "tbs", "techcrunch", "future startup", "startup bangladesh",
}

_BODY_SUBJECT_PATTERN = re.compile(
    r"\b([A-Z][a-zA-Z][a-zA-Z'’&-]*(?:\s+[A-Z][a-zA-Z'’&-]*){0,2})\s+"
    r"(?:has\s+|have\s+|just\s+|reportedly\s+|recently\s+)?"
    rf"(?:{_DEAL_VERB_ALT})\b"
)


def startup_from_body(body: str) -> str:
    """Infer the startup name from repeated deal-subject phrases in article body text."""
    if not body:
        return ""
    counts: dict[str, int] = {}
    for match in _BODY_SUBJECT_PATTERN.finditer(body[:4000]):
        cleaned = clean_startup_name(match.group(1).strip())
        if not cleaned or _is_generic_startup_name(cleaned):
            continue
        if cleaned.lower() in _BODY_NAME_NOISE:
            continue
        counts[cleaned] = counts.get(cleaned, 0) + 1
    if not counts:
        return ""
    return max(counts.items(), key=lambda kv: (kv[1], -len(kv[0])))[0]


def startup_from_investor_slug(url: str) -> str:
    """Infer a startup name from deal wording in an investor or company URL slug."""
    # finds the proper noun near the deal verb in the last url segment
    segments = [s for s in urlparse(url).path.rstrip("/").split("/") if s]
    if not segments:
        return ""
    tail = segments[-1]
    tokens = [t for t in tail.split("-") if t]
    if not tokens:
        return ""

    verb_indexes = [i for i, t in enumerate(tokens) if t.lower() in _INVESTOR_DEAL_VERBS]
    subject_anchor_verbs = {"raises", "raised", "secures", "secured", "closes", "closed"}
    verb_index = next(
        (i for i in verb_indexes if tokens[i].lower() in subject_anchor_verbs),
        verb_indexes[0] if verb_indexes else -1,
    )

    if verb_index >= 0:
        next_idx = verb_index + 1
        if next_idx < len(tokens) and tokens[next_idx].lower() in _SLUG_PREPOSITION_TOKENS:
            forward: list[str] = []
            j = next_idx + 1
            while j < len(tokens) and not _is_slug_noise(tokens[j]) and len(forward) < 3:
                forward.append(tokens[j])
                j += 1
            recovered = _name_from_tokens(forward)
            if recovered:
                return recovered
        i = verb_index - 1
        while i >= 0 and _is_slug_noise(tokens[i]):
            i -= 1
        backward: list[str] = []
        while i >= 0 and not _is_slug_noise(tokens[i]) and len(backward) < 3:
            backward.append(tokens[i])
            i -= 1
        backward.reverse()
        return _name_from_tokens(backward)

    for token in tokens:
        if not _is_slug_noise(token):
            recovered = _name_from_tokens([token])
            if recovered:
                return recovered
    return ""


_TITLE_SEPARATOR_PATTERN = re.compile(r"\s+[|–—-]\s+")
_LATIN_PROPER_NOUN_PATTERN = re.compile(r"[A-Z][a-zA-Z][a-zA-Z'’&-]+")


def _is_native_script_alias(segment: str) -> bool:
    # native script segments are usually aliases for the previous name
    if not segment:
        return False
    latin = sum(1 for ch in segment if ch.isascii() and ch.isalpha())
    non_latin = sum(1 for ch in segment if ch.isalpha() and not ch.isascii())
    return non_latin > 0 and non_latin >= latin


def _starts_with_native_script(segment: str) -> bool:
    # rejoins segments split around a native script alias
    for ch in segment:
        if ch.isspace() or not ch.isalpha():
            continue
        return not ch.isascii()
    return False


def _select_title_segment(title: str) -> str:
    if not title:
        return ""
    raw_segments = [seg.strip() for seg in _TITLE_SEPARATOR_PATTERN.split(title) if seg.strip()]
    if not raw_segments:
        return title.strip()
    # keeps native script aliases with the surrounding title segment
    segments: list[str] = []
    for seg in raw_segments:
        if segments and (_is_native_script_alias(seg) or _starts_with_native_script(seg)):
            segments[-1] = f"{segments[-1]} {seg}"
        else:
            segments.append(seg)
    if len(segments) == 1:
        return segments[0]
    qualified = [
        seg for seg in segments
        if _DEAL_VERB_PATTERN.search(seg) and _LATIN_PROPER_NOUN_PATTERN.search(seg)
    ]
    if qualified:
        return max(qualified, key=len)
    verb_segments = [seg for seg in segments if _DEAL_VERB_PATTERN.search(seg)]
    if verb_segments:
        return max(verb_segments, key=len)
    return max(segments, key=len)


def startup_from_title(title: str) -> str:
    """Infer a startup name from funding-deal wording in an article title."""
    title = re.sub(r"^In\s+\d+\s+Words:\s+", "", title, flags=re.IGNORECASE)
    cleaned = _select_title_segment(title)
    subject_patterns = [
        rf"^(.+?)\s+(?:{_TITLE_SUBJECT_VERB_ALT})\b",
        r"^(.+?)\s+(?:closes|completes|wraps)\b.+?\bround\b",
        r"^(.+?)\s+(?:gets|receives|attracts|draws)\b.+?\b(?:backing|investment|funding)\b",
        r"^(.+?)\s+announces\b.+?\bfunding\b",
        r"^(.+?)\s+receives\b.+?\binvestment\b",
        r"\b(?:funding|investment)\s+announcement:\s+(.+?)$",
    ]
    for pattern in subject_patterns:
        match = re.search(pattern, cleaned, re.IGNORECASE)
        if match:
            return clean_startup_name(match.group(1))
    object_patterns = [
        r"\binvests?\s+(?:\S+\s+){0,3}?in\s+([\w'’.& -]+?)(?:\s+(?:to|at|for|with|by|from|in|on)\b|\s+[–\-](?:\s|$)|\s*[,.]|$)",
        r"\bbacks\s+([\w'’.& -]+?)(?:\s+(?:to|at|for|with|by|from|in|on)\b|\s+[–\-](?:\s|$)|\s*[,.]|$)",
        r"\binvestment\s+in\s+([\w'’.& -]+?)(?:\s+(?:to|at|for|with|by|from|in|on)\b|\s+[–\-](?:\s|$)|\s*[,.]|$)",
    ]
    for pattern in object_patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            return clean_startup_name(match.group(1))
    return ""


_PREFIX_STRIP_PATTERNS = [
    re.compile(r"^(?:singapore|indonesia|vietnam|philippines|malaysia|thailand|bangladesh|cambodia|laos|myanmar)['’]s?\s+", re.IGNORECASE),
    re.compile(r"^(?:bangladeshi|singaporean|indonesian|malaysian|filipino|vietnamese|thai|cambodian)\s+", re.IGNORECASE),
    re.compile(r"^(?:bangladesh-based|singapore-based|indonesia-based|malaysia-based|vietnam-based|philippines-based|thailand-based)\s+", re.IGNORECASE),
    re.compile(r"^(?:[\w-]+\s+){0,4}?(?:firm|company|startup|platform|player|marketplace|app)\s+", re.IGNORECASE),
    re.compile(r"^(?:startup|fintech|edtech|healthtech|deeptech|foodtech|agritech|proptech|climatetech|biotech|insurtech)\s+", re.IGNORECASE),
]

# helper verb fragments left by greedy title capture
_TRAILING_FRAGMENT_PATTERN = re.compile(
    r"\s+(?:has|have|had|is|are|was|were|will|just|finally|reportedly)$",
    re.IGNORECASE,
)


def clean_startup_name(value: str) -> str:
    """Remove geography, prose fragments, and generic words from a startup name."""
    value = value.strip()
    value = re.sub(
        r"^(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
        r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|"
        r"Dec(?:ember)?)\s+\d{1,2},\s+\d{4}\s+",
        "",
        value,
        flags=re.IGNORECASE,
    )
    value = re.sub(r"^(?:\d{4}[-/]\d{1,2}[-/]\d{1,2}\s+)?Portfolio News\s+", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+Portfolio News$", "", value, flags=re.IGNORECASE)
    value = re.sub(r"^[A-Z][\w'’&.-]+-backed\s+", "", value)
    value = re.sub(r"\bjoins?\b.+$", "", value, flags=re.IGNORECASE).strip()
    value = re.sub(r"\blaunch(?:es|ed)?\b.+$", "", value, flags=re.IGNORECASE).strip()
    value = re.sub(r"\bannounces?\b.+$", "", value, flags=re.IGNORECASE).strip()
    # trims descriptive geography from captured startup names
    geo_split = re.search(
        r"(?:^|\s+)(?:southeast\s+asia|south-?east\s+asia|south\s+asia|apac|asia)\b",
        value,
        flags=re.IGNORECASE,
    )
    if geo_split:
        value = value[: geo_split.start()].strip()
    # repeats prefix stripping because prefixes can be stacked
    while True:
        previous = value
        for pattern in _PREFIX_STRIP_PATTERNS:
            value = pattern.sub("", value)
        if value == previous:
            break
    # removes trailing helper verbs left by greedy subject capture
    while True:
        previous = value
        value = _TRAILING_FRAGMENT_PATTERN.sub("", value).strip()
        if value == previous:
            break
    # removes trailing native script aliases
    value = re.sub(r"\s+[^\s\x00-\x7f]+\s*$", "", value).strip()
    # keeps only the proper noun after leading descriptive prefixes
    descriptive_prefix = re.match(
        r"^(?:country['’]s\s+(?:first|leading|largest|biggest|top)\s+|the\s+(?:first|leading|largest|biggest|top)\s+|leading\s+|first\s+)[\w\s'’&-]*?\s+([A-Z][\w'’&-]+(?:\s+[A-Z][\w'’&-]+)?)\s*$",
        value,
    )
    if descriptive_prefix:
        value = descriptive_prefix.group(1)
    value = re.sub(r"\s+(?:ai|ml|app|io)$", "", value, flags=re.IGNORECASE)
    cleaned = value.strip(" :,-")
    if _is_generic_startup_name(cleaned):
        return ""
    return cleaned


_GENERIC_STARTUP_NAMES = {
    "local startups", "local startup", "multiple startups", "startups",
    "the startup", "the company", "the firm", "the business",
    "portfolio", "portfolio archive", "funding", "investment",
    "various startups", "several startups", "two startups", "three startups",
    "firm", "company", "startup", "platform", "player", "marketplace",
    "business", "venture", "fund", "round", "deal",
    "southeast asia", "south-east asia", "asia", "region", "southeast", "south-east",
    "climate investment", "venture capital", "growth stage",
    # investor names that can be mistaken for startups in portfolio posts
    "openspace", "wavemaker", "monkshill", "vertex", "insignia",
    "jungle", "golden gate", "peak xv", "sequoia", "east ventures",
}


def _is_generic_startup_name(value: str) -> bool:
    if not value:
        return True
    return value.lower() in _GENERIC_STARTUP_NAMES
