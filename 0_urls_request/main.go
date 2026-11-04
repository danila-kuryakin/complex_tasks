package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
)

/*
First Successful Request
Цель
Получить первый успешный HTTP-ответ от нескольких URL, выполняя запросы параллельно.

Условие
Функция получает список URL. Для каждого URL нужно выполнить HTTP GET. Успешным считается ответ со статусом от 200 до 299 включительно. Вернуть нужно тело первого успешного ответа и URL, с которого он получен.

Требуемый API
type FetchResult struct {
    URL  string
    Body []byte
}

func FirstSuccessfulRequest(
    ctx context.Context,
    client *http.Client,
    urls []string,
) (FetchResult, error)
Функциональные требования
Запросы к переданным URL выполняются параллельно.
Функция возвращает первый ответ с успешным HTTP-статусом.
Если все запросы завершились ошибкой или неуспешным HTTP-статусом, функция возвращает ошибку.
Отмена переданного контекста прекращает ожидание результата и возвращается как ошибка.
Для HTTP-статуса вне диапазона 200–299 результат считается неуспешным.
*/

type FetchResult struct {
	URL  string
	Body []byte
}

func FirstSuccessfulRequest(
	ctx context.Context,
	client *http.Client,
	urls []string,
) (FetchResult, error) {
	if len(urls) == 0 {
		return FetchResult{}, errors.New("URLs is empty")
	}

	ctx, cansel := context.WithCancel(ctx)
	defer cansel()

	type dataFetch struct {
		fetch FetchResult
		err   error
	}

	wg := &sync.WaitGroup{}
	ch := make(chan dataFetch, len(urls))

	for _, url := range urls {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				ch <- dataFetch{err: err}
				return
			}

			resp, err := client.Do(req)
			if err != nil {
				ch <- dataFetch{err: err}
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode < 200 || resp.StatusCode > 299 {
				ch <- dataFetch{err: fmt.Errorf("%s: status code 2xx: %w", url, err)}
				return
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				ch <- dataFetch{err: err}
				return
			}

			ch <- dataFetch{
				fetch: FetchResult{
					URL:  url,
					Body: body,
				},
				err: nil,
			}
		}(url)
	}

	var lastErr error

	for i := 0; i < len(urls); i++ {
		select {
		case out := <-ch:
			if out.err == nil {
				return out.fetch, nil
			}
			lastErr = out.err
		case <-ctx.Done():
			return FetchResult{}, ctx.Err()
		}
	}

	if lastErr != nil {
		return FetchResult{}, lastErr
	}

	return FetchResult{}, errors.New("all requests failed")
}

func main() {
	urls := []string{
		"http://vk.ru/",
		"http://ok.ru/",
		"http://ya.ru/",
	}

	request, err := FirstSuccessfulRequest(context.Background(), &http.Client{}, urls)
	if err != nil {
		return
	}

	fmt.Println(request.URL)
}
