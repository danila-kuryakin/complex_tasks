package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
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
		return FetchResult{}, errors.New("urls is empty")
	}

	ctx, cansel := context.WithCancel(ctx)
	defer cansel()

	type retData struct {
		data FetchResult
		err  error
	}

	ch := make(chan retData, len(urls))

	for _, u := range urls {
		go func(url string) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				ch <- retData{err: err}
				return
			}

			resp, err := client.Do(req)
			if err != nil {
				ch <- retData{err: err}
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode < 200 || resp.StatusCode > 299 {
				ch <- retData{err: fmt.Errorf("Error in %s: status coder is not 2xx", url)}
				return
			}

			data, err := io.ReadAll(resp.Body)
			if err != nil {
				ch <- retData{err: err}
				return
			}

			ch <- retData{
				data: FetchResult{
					URL:  url,
					Body: data,
				},
				err: nil,
			}
		}(u)
	}

	var lastErr error
	for range urls {
		select {
		case out := <-ch:
			if out.err == nil {
				return out.data, nil
			}
			return FetchResult{}, lastErr
		case <-ctx.Done():
			return FetchResult{}, ctx.Err()
		}
	}

	if lastErr != nil {
		return FetchResult{}, lastErr
	}

	return FetchResult{}, errors.New("urls is end")
}

func main() {
	urls := []string{
		"http://vk.ru/",
		"http://ok.ru/",
		"http://ya.ru/",
	}

	request, err := FirstSuccessfulRequest(context.Background(), &http.Client{Timeout: 5 * time.Second}, urls)
	if err != nil {
		return
	}

	fmt.Println(request.URL)
}
