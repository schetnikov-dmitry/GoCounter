# GoCounter

Программа предназначена для подсчета количества слов "Go"
на веб страницах, ссылки которых переданы на стандартный вход.


## Пример применения

```
$ for i in {1..3}; do echo -n -e "https://golang.org\n"; done | go run GoCounter.go
2018/07/05 21:54:38 (worker2): Count of words "Go" on the page https://golang.org: 9
2018/07/05 21:54:38 (worker1): Count of words "Go" on the page https://golang.org: 9
2018/07/05 21:54:38 (worker0): Count of words "Go" on the page https://golang.org: 9
2018/07/05 21:54:38 Total: 27
2018/07/05 21:54:38 no more work
```