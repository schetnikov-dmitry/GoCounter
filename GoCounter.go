package main

import (
	"bufio"
	"container/heap"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
)

const (
	// признак окончания работы
	NO_MORE_LINE string = "NO_MORE_LINE"
	// что ищем
	SEARCH string = "Go"
)

// обработчик веб ссылки
// data - ссылка
// workername - имя воркера (для красоты)
func handle_data(data string, workername string) (result int) {

	// здесь будет количество вхождений
	result = 0

	response, err := http.Get(data)
	if err != nil {
        	log.Println("http.Get error (", workername, "):", err)
        	return
    	}

        defer response.Body.Close()

	count := 0
	scanner := bufio.NewScanner(response.Body)

	for scanner.Scan() {
		count += strings.Count(scanner.Text(), SEARCH)
	}

	log.Printf("(%s): Count of words \"%s\" on the page %s: %d", workername, SEARCH, data, count)

	// все что нашли, вернули
	result = count

	return
}

// чтение стандартного входа
// chan_balancer - канал балансировщкиа
func read_stdin(chan_balancer chan string) (err error) {

	var line string
	var eof bool = false

	// читаем Stdin
	reader := bufio.NewReader(os.Stdin)

	for !eof {

		// читаем строку
		line, err = reader.ReadString('\n')
		if err != nil {
			eof = true
			continue
		}

		// проверяем переносы строк
		line = strings.TrimSuffix(line, "\n")
		line = strings.TrimSuffix(line, "\r")

		// проверяем: а есть что передать?
		if len(line) == 0 {
			continue
		}

		// отправляем балансировщику
		chan_balancer <- line
	}

	// все что можно отправили, сообщаем балансировщику что больше ничего не будет
	chan_balancer <- NO_MORE_LINE
	return
}

func main () {

	// количество воркеров
	var WORKERS int = 5
	// глубина очереди воркера
	var WORKERSCAP int = 1

	// а вдруг захотим чего-то другого?
	flag.IntVar(&WORKERS, "workers", WORKERS, "number of workers")
	flag.IntVar(&WORKERSCAP, "capacity", WORKERSCAP, "capacity of workers")
	flag.Parse()

	// Подготовим балансировщик и необходимые ему каналы
	chan_balancer := make(chan string)
	chan_balancer_quit := make(chan int)
	balancer := new(Balancer)
	balancer.init(WORKERS, WORKERSCAP, chan_balancer)
	go balancer.balance(chan_balancer_quit)

	// канал и сигнал остановки
	keys := make(chan os.Signal, 1)
	signal.Notify(keys, os.Interrupt)

	// читаем со стандартного входа
	go func(chan_balancer chan string) {
		err:= read_stdin(chan_balancer)
		if (err != nil) && (err != io.EOF) {
			log.Println("Read StdIn error", err)
		}
	}(chan_balancer)

	//Основной цикл программы:
	for {
		select {
		// пришла информация от нотификатора сигналов:
		case <-keys:
			log.Println("CTRL-C")
			return
		// пришла информация об окончании работы балансировщика, с результатом:
		case total := <-chan_balancer_quit:
			log.Println("Total:", total)
			log.Println("no more work")
			return
		}
	}
	return
}

// Балансировка

// Рабочий
type Worker struct {
	name            string             // имя воркера
	data            chan string	   // канал для заданий
	pending		int                // кол-во оставшихся задач
	index           int                // позиция в куче
	result          int                // результат выполнения
}

// Вызов обработчика задания
func (worker *Worker) work(done chan *Worker) {
	for {
		data := <-worker.data  	// читаем следующее задание
		worker.result = handle_data(data, worker.name)      // обработка задания
		done <- worker       	// показываем что завершили работу
	}
}

// Это наша "куча" воркеров:
type Pool []*Worker

// Проверка: кто меньше - в нашем случае меньше тот у кого меньше заданий:
func (pool Pool) Less(i, j int) bool {
	return pool[i].pending < pool[j].pending
}

// Количество рабочих в пуле:
func (pool Pool) Len() int {
	return len(pool)
}

// Реализуем обмен местами:
func (pool Pool) Swap(i, j int) {
	if i >= 0 && i < len(pool) && j >= 0 && j < len(pool) {
		pool[i], pool[j] = pool[j], pool[i]
		pool[i].index, pool[j].index = i, j
	}
}

//Заталкивание элемента:
func (pool *Pool) Push(x interface{}) {
	n := len(*pool)
	worker := x.(*Worker)
	worker.index = n
	*pool = append(*pool, worker)
}

// И выталкивание:
func (pool *Pool) Pop() interface{} {
	old := *pool
	n := len(old)
	item := old[n - 1]
	item.index = -1
	*pool = old[0 : n - 1]
	return item
}

// Балансировщик
type Balancer struct {
	pool             Pool               // "куча" рабочих
	done             chan *Worker       // Канал уведомления о завершении для рабочих
	datas            chan string        // Канал для получения новых заданий
	flowctrl         chan bool          // Канал для Flow Control
	Queue            int                // Количество незавершенных заданий переданных рабочим
	Count_workers    int                // количество рабочих
	Count_workerscap int                // размер очереди каждого рабочего
}

// Инициализируем балансировщик. Аргументы: количество работников, количество задач для одного работника, канал по которому приходят задания
func (balancer *Balancer) init(WORKERS int, WORKERSCAP int, in chan string) {

	balancer.datas = make(chan string)
	balancer.flowctrl = make(chan bool)
	balancer.done = make(chan *Worker)

	balancer.Count_workers = WORKERS
	balancer.Count_workerscap = WORKERSCAP

	//Запускаем Flow Control
	go func() {
		for {
			balancer.datas <- <-in //получаем новое задание и пересылаем его на внутренний канал
			<-balancer.flowctrl    //а потом ждем получения подтверждения
		}
	}()

	// Инициализируем кучу
	heap.Init(&balancer.pool)
}

//Рабочая функция балансировщика получает аргументом канал уведомлений от главного цикла
func (balancer *Balancer) init_worker(number int) {
	worker := &Worker{
		name:    "worker" + strconv.Itoa(number),
		data:    make(chan string, balancer.Count_workerscap),
		index:   0,
		pending: 0,
	}

	go worker.work(balancer.done)     //запускаем рабочего
	heap.Push(&balancer.pool, worker) //и заталкиваем его в кучу
}

//Рабочая функция балансировщика. Аргумент канал уведомления об окончании работы
func (balancer *Balancer) balance(quit chan int) {

	noMoreLine := false // Флаг завершения, поднимаем когда кончились задания
	var total int = 0

	for {
		select { //В цикле ожидаем коммуникации по каналам:

		case data := <-balancer.datas:
			// Получена новая строка

			// Если конец работы, посылаем сигнал выхода
			if data == NO_MORE_LINE {
				noMoreLine = true
				// очереди нет
				if balancer.Queue == 0 {
					quit <- total // можно отправлять сигнал выхода
				}
			} else {

				// создаем нового рабочего, но не лишнего
				poolLength := len(balancer.pool)
				if (poolLength < balancer.Count_workers) && (poolLength <= balancer.Queue) {
					balancer.init_worker(poolLength)
				}
				balancer.dispatch(data) // отправляем рабочим
			}

		case worker := <-balancer.done:
			// пришло уведомление, что рабочий закончил загрузку
			total += worker.result
			balancer.completed(worker) // обновляем его данные
			if noMoreLine {
				// очереди нет
				if balancer.Queue == 0 {
					quit <- total // можно отправлять сигнал выхода
				}
			}
		}
	}
}

// Функция отправки задания
func (balancer *Balancer) dispatch(data string) {
	worker := heap.Pop(&balancer.pool).(*Worker) 	// Берем из кучи самого незагруженного рабочего
	worker.data <- data                    		// отправляем ему задание
	worker.pending++                      		// Добавляем ему "вес"
	heap.Push(&balancer.pool, worker)               // отправляем назад в кучу

	// можно еще читать
	if balancer.Queue++; balancer.Queue < balancer.Count_workers * balancer.Count_workerscap {
		balancer.flowctrl <- true
	}
}

// Обработка завершения задания
func (balancer *Balancer) completed(w *Worker) {
	w.pending--				// Уменьшаем "вес" рабочему
	// обновляем информацию о работнике в куче
	heap.Remove(&balancer.pool, w.index)
	heap.Push(&balancer.pool, w)

	// можно еще читать
	if balancer.Queue--; balancer.Queue == balancer.Count_workers * balancer.Count_workerscap - 1 {
		balancer.flowctrl <- true
	}
}