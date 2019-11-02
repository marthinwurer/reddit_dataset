if .is_self or .score < 3 or (.url | test(".*\\.(jpg|png)") | not) then
    empty
else
    {score, over_18, title, url, subreddit, domain}
end
