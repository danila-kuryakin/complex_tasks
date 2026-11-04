### 1. Rate Limiter

Реализовать:

```go
type Limiter interface {
    Allow(key string) bool
}
```

Требование:

> Для каждого пользователя разрешить не более N запросов за T секунд.

Например:

```text
user-1 → 100 req / 1 min
user-2 → 100 req / 1 min
```

Требования:

* потокобезопасность;
* несколько пользователей;
* автоматическое удаление неактивных пользователей;
* не создавать goroutine на каждый запрос;
* корректно работать при высокой конкуренции.

**Усложнение:** сделать token bucket.

**Еще усложнение:** добавить глобальный лимит поверх user-level:

```text
user: 100 req/min
global: 10 000 req/min
```

Rate limiter и token bucket — типичные concurrency/system coding tasks. ([GitHub][2])

---

### 2. TTL Cache

```go
type Cache[K comparable, V any] interface {
    Set(key K, value V, ttl time.Duration)
    Get(key K) (V, bool)
    Delete(key K)
}
```

Требования:

* `Get/Set/Delete` безопасны конкурентно;
* TTL;
* просроченные элементы не должны возвращаться;
* background cleanup;
* cleanup не должен создавать goroutine для каждого элемента;
* настроить период cleanup;
* graceful shutdown.

**Усложнение:** сделать `Get` максимально дешевым для read-heavy нагрузки.

TTL cache прямо присутствует среди Go interview problems. ([GitHub][1])

---

### 3. Web Crawler

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

**Усложнения:**

* ограничение RPS на домен;
* retry;
* graceful shutdown;
* ограничение глубины обхода.

Web crawler также присутствует в подборке реальных Go live-coding задач. ([GitHub][1])

---

### 4. API Client Manager

Есть внешний API:

```go
func DoRequest(ctx context.Context, req Request) (Response, error)
```

У API ограничения:

```text
не более 10 одновременно выполняющихся запросов
не более 100 запросов в минуту
```

Нужно написать менеджер:

```go
type ClientManager struct {
    // ...
}

func (m *ClientManager) Do(ctx context.Context, req Request) (Response, error)
```

Он должен:

* принимать большое количество запросов;
* ставить их в очередь;
* соблюдать concurrency limit;
* соблюдать rate limit;
* не терять запросы;
* поддерживать cancellation;
* корректно завершаться;
* максимально использовать доступную производительность.

---

### 5. Request Batcher

Есть поток запросов:

```go
Process(item)
```

Но downstream API умеет принимать только batch:

```go
ProcessBatch([]Item)
```

Нужно написать агрегатор.

Правила:

* отправлять batch, когда накопилось **N элементов**;
* либо когда прошло **50 ms** с первого элемента;
* не допускать бесконечного ожидания;
* несколько producers;
* graceful shutdown;
* все накопленные элементы должны быть обработаны.

Это уже заставляет разобраться с:

* channels;
* `select`;
* ticker/timer;
* batching;
* cancellation;
* race conditions.

Batch aggregator также встречается среди production-style interview задач. ([GitHub][2])

---

### 6. First Successful Request

Есть 5 независимых источников:

```text
Server A
Server B
Server C
Server D
Server E
```

Нужно получить **первый успешный ответ**.

Требования:

* запросы выполняются параллельно;
* как только один успешно ответил — вернуть результат;
* остальные запросы отменить;
* если все завершились ошибкой — вернуть aggregate error;
* timeout;
* не оставить goroutines после завершения.


[1]: https://github.com/blindlobstar/go-interview-problems?utm_source=chatgpt.com "GitHub - blindlobstar/go-interview-problems: Go live coding interview problems · GitHub"
[2]: https://github.com/ombharatiya/FAANG-Coding-Interview-Questions/blob/main/AI-Companies-Interview-Questions.md?utm_source=chatgpt.com "FAANG-Coding-Interview-Questions/AI-Companies-Interview-Questions.md at main · ombharatiya/FAANG-Coding-Interview-Questions · GitHub"
[3]: https://gist.github.com/sauravexodus/0fbdfd92c58033ed3e605d1260ee4516?utm_source=chatgpt.com "Tests for API throughput maximizer · GitHub"
[4]: https://www.skillveris.com/interview-questions/go/how-do-you-design-a-worker-pool-with-backpressure-in-go?utm_source=chatgpt.com "Designing a Go Worker Pool With Real Backpressure | SkillVeris"

