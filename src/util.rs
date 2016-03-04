pub fn parse_update_count(tag: String) -> u64 {
    tag.split(' ').last().unwrap().parse().unwrap_or(0)
}
