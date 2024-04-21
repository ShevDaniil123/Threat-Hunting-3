# Лабораторная работа №4
Шевченко Д. Д.

## Цель Работы

1.  Изучить возможности СУБД DuckDB для обработки и анализ больших
    данных
2.  Получить навыки применения DuckDB совместно с языком
    программирования R
3.  Получить навыки анализа метаинфомации о сетевом трафике
4.  Получить навыки применения облачных технологий хранения, подготовки
    и анализа данных: Yandex Object Storage, Rstudio Server.

## Ход работы

## Подключение arrow

``` r
library(duckdb)
```

    Loading required package: DBI

``` r
library(dplyr)
```


    Attaching package: 'dplyr'

    The following objects are masked from 'package:stats':

        filter, lag

    The following objects are masked from 'package:base':

        intersect, setdiff, setequal, union

``` r
library(tidyverse)
```

    ── Attaching core tidyverse packages ──────────────────────── tidyverse 2.0.0 ──
    ✔ forcats   1.0.0     ✔ readr     2.1.5
    ✔ ggplot2   3.4.4     ✔ stringr   1.5.1
    ✔ lubridate 1.9.3     ✔ tibble    3.2.1
    ✔ purrr     1.0.2     ✔ tidyr     1.3.1

    ── Conflicts ────────────────────────────────────────── tidyverse_conflicts() ──
    ✖ dplyr::filter() masks stats::filter()
    ✖ dplyr::lag()    masks stats::lag()
    ℹ Use the conflicted package (<http://conflicted.r-lib.org/>) to force all conflicts to become errors

``` r
library(lubridate)
```

``` r
c <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
dbExecute(conn = c, "INSTALL httpfs; LOAD httpfs;")
```

    [1] 0

``` r
PARQUET_FILE1 = "https://storage.yandexcloud.net/arrow-datasets/tm_data.pqt"


a <- "SELECT * FROM read_parquet([?])"
dt <- dbGetQuery(c, a, list(PARQUET_FILE1))
```

## Задание 1

### Важнейшие документы с результатами нашей исследовательской деятельности в области создания вакцин скачиваются в виде больших заархивированных дампов. Один из хостов в нашей сети используется для пересылки этой информации – он пересылает гораздо больше информации на внешние ресурсы в Интернете, чем остальные компьютеры нашей сети. Определите его IP-адрес.

``` r
dt1 <- dt  %>% select(src, dst, bytes) %>% filter(!str_detect(dst, '1[2-4].*')) %>% group_by(src) %>% summarise(bytes_amount = sum(bytes)) %>% arrange(desc(bytes_amount)) %>% collect()
dt1 %>% head(1)
```

    # A tibble: 1 × 2
      src          bytes_amount
      <chr>               <dbl>
    1 13.37.84.125   5765792351

### Ответ: 13.37.84.125

## Задание 2

### Другой атакующий установил автоматическую задачу в системном планировщике cron для экспорта содержимого внутренней wiki системы. Эта система генерирует большое количество трафика в нерабочие часы, больше чем остальные хосты. Определите IP этой системы. Известно, что ее IP адрес отличается от нарушителя из предыдущей задачи.

### Поиск не рабочих часов

``` r
dt21 <- dt %>% select(timestamp, src, dst, bytes) %>% filter(!str_detect(dst, '1[2-4].*')) %>% mutate(timestamp = hour(as_datetime(timestamp/1000))) %>% group_by(timestamp) %>% summarize(traffic_size = sum(bytes)) %>% arrange(desc(traffic_size))
dt21 %>% collect() %>% print(n = Inf)
```

    # A tibble: 24 × 2
       timestamp traffic_size
           <int>        <dbl>
     1        18  60193966072
     2        23  60192411947
     3        21  60168340116
     4        16  60098320900
     5        20  60080805313
     6        17  60038805616
     7        22  60019583499
     8        19  59993406253
     9         7   2407989038
    10        12   2273682799
    11         3   2272781208
    12         6   2272628627
    13         0   2272231719
    14        13   2269391474
    15         8   2256895552
    16        15   2256892969
    17         9   2255747421
    18         5   2254830735
    19        14   2253404224
    20         2   2250935353
    21         4   2247503973
    22        10   2246424468
    23        11   2245261098
    24         1   2241313453

### Поиск IP

``` r
dt22 <- dt %>% select(timestamp, src, dst, bytes) %>% mutate(timestamp = hour(as_datetime(timestamp/1000))) %>% filter(!str_detect(dst, '1[2-4].*') & timestamp >= 0 & timestamp <= 15)  %>% group_by(src) %>% summarise(bytes_amount = sum(bytes)) %>% arrange(desc(bytes_amount)) %>% collect()
dt22 %>% filter(src != '13.37.84.125') %>% head(1)
```

    # A tibble: 1 × 2
      src         bytes_amount
      <chr>              <int>
    1 12.55.77.96    194447613

### Ответ: 12.55.77.96

## Задание 3

### Еще один нарушитель собирает содержимое электронной почты и отправляет в Интернет используя порт, который обычно используется для другого типа трафика. Атакующий пересылает большое количество информации используя этот порт, которое нехарактерно для других хостов, использующих этот номер порта. Определите IP этой системы. Известно, что ее IP адрес отличается от нарушителей из предыдущих задач.

Найдем среднее значение трафика для каждого порта, и суммарный объем
трафика для каждого хоста по портам. Затем соединим две таблицы, и
найдем отношение максимального значения трафика по портам к среднему
значению объема трафику по порту

``` r
dt31 <- dt |> select(timestamp, src, dst, port, bytes) %>% filter(!str_detect(dst, '1[2-4].')) %>% group_by(src, port) %>% summarise(bytes_ip_port = sum(bytes)) %>% group_by(port) %>% summarise(average_port_traffic = mean(bytes_ip_port)) %>% arrange(desc(average_port_traffic)) |> collect()
```

    `summarise()` has grouped output by 'src'. You can override using the `.groups`
    argument.

``` r
dt32 <- dt |> select(timestamp, src, dst, port, bytes) %>% filter(!str_detect(dst, '1[2-4].')) %>% group_by(src, port) %>% summarise(bytes_ip_port = sum(bytes)) %>% collect() %>% group_by(port) %>% top_n(1, bytes_ip_port) %>% arrange(desc(bytes_ip_port))
```

    `summarise()` has grouped output by 'src'. You can override using the `.groups`
    argument.

``` r
dt33 <- merge(dt32, dt31, by = "port")

anomaly_ip_port_traffic <- dt33 %>% mutate(average_anomaly = bytes_ip_port/average_port_traffic) %>% arrange(desc(average_anomaly)) %>% head(1)
anomaly_ip_port_traffic
```

      port         src bytes_ip_port average_port_traffic average_anomaly
    1  124 12.30.96.87        281993             15641.06        18.02902

### Ответ: 12.30.96.87

## Задание 4

### Зачастую в корпоротивных сетях находятся ранее зараженные системы, компрометация которых осталась незамеченной. Такие системы генерируют небольшое количество трафика для связи с панелью управления бот-сети, но с одинаковыми параметрами – в данном случае с одинаковым номером порта. Какой номер порта используется бот-панелью для управления ботами?

``` r
a <- "SELECT min(bytes),max(bytes),max(bytes) - min(bytes), avg(bytes), port,count(port) FROM read_parquet([?]) group by port having avg(bytes) - min(bytes) < 10 and min(bytes) != max(bytes)"
dt <- dbGetQuery(c, a, list(PARQUET_FILE1))
dt %>% select(port)
```

      port
    1  124

### Ответ: 124

## Вывод

По завершению работы удалось изучить возможности СУБД DuckDB для
обработки и анализ больших данных, получить навыки применения DuckDB
совместно с языком программирования R, получить навыки анализа
метаинфомации о сетевом трафике, получить навыки применения облачных
технологий хранения, подготовки и анализа данных: Yandex Object Storage,
Rstudio Server.
