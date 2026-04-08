---
name: Firecrawl reliability patterns
description: Which Firecrawl strategies work reliably per site, and known failure modes
type: project
---

8vc: `scrape_extract` via /v1/scrape with formats=[markdown, extract] works reliably (20 jobs consistently).

greycroft: async `/v1/extract` is flaky — intermittently returns empty `jobs: []`. Should be switched to `scrape_extract` strategy.

weworkremotely: persistent 408 timeouts on most category pages via /v1/scrape. The `remote-design-jobs` category is critically important but consistently times out. The all-jobs search page (135K chars markdown) is too large. Individual category pages sometimes succeed but are unreliable.

**Why:** Firecrawl's scrape+extract has server-side processing limits that vary by page size and load. The standalone `/v1/extract` endpoint is async but returns empty results intermittently.

**How to apply:** Prefer `scrape_extract` strategy over async `/v1/extract`. For large pages, consider scraping markdown first then extracting locally, or splitting into smaller requests.
