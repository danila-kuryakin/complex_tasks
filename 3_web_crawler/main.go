package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"golang.org/x/net/html"
)

/*
3. Web Crawler

Дать URL:

```text
https://example.com
```

Нужно собрать все страницы в пределах одного домена.

Ограничения:

* максимум 10 одновременно загружаемых страниц;
* URL не должен обрабатываться дважды;
* timeout на HTTP-запрос;
* остановка через context;
* ошибки одной страницы не должны останавливать crawler;
* корректно дождаться всех workers.
*/

type Crawler struct {
	rootHost string
	client   *http.Client
	visited  map[string]bool
	mu       sync.Mutex
	sem      chan struct{}
	wg       sync.WaitGroup
}

func NewCrawler(maxConcurrency int, timeout time.Duration) *Crawler {
	return &Crawler{
		client: &http.Client{
			Timeout: timeout,
		},
		visited: make(map[string]bool),
		sem:     make(chan struct{}, maxConcurrency),
	}
}

func (c *Crawler) Crawl(ctx context.Context, startURL string) error {
	parsedStart, err := url.Parse(startURL)
	if err != nil {
		return fmt.Errorf("invalid start url: %w", err)
	}
	c.rootHost = parsedStart.Host

	c.wg.Add(1)
	go c.crawlRecursive(ctx, startURL)

	c.wg.Wait()
	return nil
}

func (c *Crawler) crawlRecursive(ctx context.Context, rawURL string) {
	defer c.wg.Done()

	if ctx.Err() != nil {
		return
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		return
	}

	parsed.Fragment = ""
	normalizedURL := parsed.String()

	if parsed.Host != c.rootHost {
		return
	}

	c.mu.Lock()
	if c.visited[normalizedURL] {
		c.mu.Unlock()
		return
	}
	c.visited[normalizedURL] = true
	c.mu.Unlock()

	select {
	case c.sem <- struct{}{}:
		defer func() { <-c.sem }()
	case <-ctx.Done():
		return
	}

	fmt.Printf("Load: %s\n", normalizedURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, normalizedURL, nil)
	if err != nil {
		return
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return
	}

	links, err := extractLinks(resp.Body, parsed)
	if err != nil {
		return
	}

	for _, link := range links {
		select {
		case <-ctx.Done():
			return
		default:
			c.wg.Add(1)
			go c.crawlRecursive(ctx, link)
		}
	}
}

func extractLinks(body io.ReadCloser, baseURL *url.URL) ([]string, error) {
	doc, err := html.Parse(body)
	if err != nil {
		return nil, err
	}

	var links []string
	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, attr := range n.Attr {
				if attr.Key == "href" {
					u, err := url.Parse(attr.Val)
					if err != nil {
						continue
					}

					resolved := baseURL.ResolveReference(u)
					resolved.Fragment = ""
					links = append(links, resolved.String())
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(doc)
	return links, nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	crawler := NewCrawler(10, 5*time.Second)

	startURL := "http://quotes.toscrape.com/"
	fmt.Printf("Starting crawler for: %s\n", startURL)

	if err := crawler.Crawl(ctx, startURL); err != nil {
		fmt.Printf("Crawler finished with error: %v\n", err)
	}

	fmt.Println("Crawling successfully completed. All workers stopped.")
}
