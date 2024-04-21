# Лабораторная работа №5
Шевченко Д. Д.

## Цель работы

1.  Изучить возможности СУБД Clickhouse для обработки и анализ больших
    данных
2.  Получить навыки применения Clickhouse совместно с языком
    программирования R
3.  Получить навыки анализа метаинфомации о сетевом трафике
4.  Получить навыки применения облачных технологий хранения, подготовки
    и анализа данных: Managed Service for ClickHouse, Rstudio Server.

## Ход работы

### Подготовка

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
library(DBI)
```

``` r
library(lubridate)
```

    install.packages("ClickHouseHTTP")

``` r
library(ClickHouseHTTP)

## HTTP connection
con <- dbConnect(
   ClickHouseHTTP::ClickHouseHTTP(), 
   host="rc1d-sbdcf9jd6eaonra9.mdb.yandexcloud.net",
                      port=8443,
                      user="student24dwh",
                      password = "DiKhuiRIVVKdRt9XON",
                      db = "TMdata",
   https=TRUE, ssl_verifypeer=FALSE)
```

``` r
dataFromDB <- dbReadTable(con, "data")
dff <- dbGetQuery(con, "SELECT * FROM data")
```

#### 1. Найдите утечку данных из Вашей сети

Важнейшие документы с результатами нашей исследовательской деятельности
в области создания вакцин скачиваются в виде больших заархивированных
дампов. Один из хостов в нашей сети используется для пересылки этой
информации – он пересылает гораздо больше информации на внешние ресурсы
в Интернете, чем остальные компьютеры нашей сети. Определите его
IP-адрес.

``` r
dl1 <- dff  %>% select(src, dst, bytes) %>% filter(!str_detect(dst, '1[2-4].*')) %>% group_by(src) %>% summarise(bytes_amount = sum(bytes)) %>% arrange(desc(bytes_amount)) %>% collect()
dl1 %>% head(1)
```

    # A tibble: 1 × 2
      src          bytes_amount
      <chr>               <dbl>
    1 13.37.84.125   5765792351

Ответ: 13.37.84.125

#### 2. Найдите утечку данных 2

Другой атакующий установил автоматическую задачу в системном
планировщике cron для экспорта содержимого внутренней wiki системы. Эта
система генерирует большое количество трафика в нерабочие часы, больше
чем остальные хосты. Определите IP этой системы. Известно, что ее IP
адрес отличается от нарушителя из предыдущей задачи.

``` r
dl2 <- dff %>%
    select(timestamp, src, dst, bytes) %>%
    mutate(trafic = (str_detect(src, "^((12|13|14)\\.)") & !str_detect(dst, "^((12|13|14)\\.)")),time = hour(as_datetime(timestamp/1000))) %>%
    filter(trafic == TRUE, time >= 0 & time <= 24) %>% group_by(time) %>%
    summarise(trafictime = n()) %>% arrange(desc(trafictime))

dl2 |> collect()
```

    # A tibble: 24 × 2
        time trafictime
       <int>      <int>
     1    16    4490576
     2    22    4489703
     3    18    4489386
     4    23    4488093
     5    19    4487345
     6    21    4487109
     7    17    4483578
     8    20    4482712
     9    13     169617
    10     7     169241
    # ℹ 14 more rows

``` r
dl3 <- dff %>% mutate(time = hour(as_datetime(timestamp/1000))) %>% 
  filter(!str_detect(src, "^13.37.84.125")) %>% 
  filter(str_detect(src, "^12.") | str_detect(src, "^13.") | str_detect(src, "^14."))  %>%
  filter(!str_detect(dst, "^12.") | !str_detect(dst, "^13.") | !str_detect(dst, "^14."))  %>%
  filter(time >= 1 & time <= 15) %>% 
  group_by(src) %>% summarise("sum" = sum(bytes)) %>%
  filter(sum > 290000000) %>% select(src,sum) 

dl3 |> collect()
```

    # A tibble: 1 × 2
      src               sum
      <chr>           <int>
    1 12.55.77.96 298669501

Ответ: 12.55.77.96

#### 3. Найдите утечку данных 3

Еще один нарушитель собирает содержимое электронной почты и отправляет в
Интернет используя порт, который обычно используется для другого типа
трафика. Атакующий пересылает большое количество информации используя
этот порт, которое нехарактерно для других хостов, использующих этот
номер порта. Определите IP этой системы. Известно, что ее IP адрес
отличается от нарушителей из предыдущих задач.

``` r
dl4 <- dff %>% filter(!str_detect(src, "^13.37.84.125")) %>% 
  filter(!str_detect(src, "^12.55.77.96")) %>% 
  filter(str_detect(src, "^12.") | str_detect(src, "^13.") | str_detect(src, "^14."))  %>%
  filter(!str_detect(dst, "^12.") | !str_detect(dst, "^13.") | !str_detect(dst, "^14."))  %>% select(src, bytes, port) 


dl5 <- dl4 %>%  group_by(port) %>% summarise("mean"=mean(bytes), "max"=max(bytes), "sum" = sum(bytes)) %>% 
  mutate("Raz"= max-mean)  %>% filter(Raz!=0, Raz>170000)

dl5 |> collect()
```

    # A tibble: 1 × 5
       port   mean    max         sum     Raz
      <int>  <dbl>  <int>       <dbl>   <dbl>
    1    37 33348. 209402 48192673159 176054.

``` r
dl6 <- dl4  %>% filter(port == 37) %>% group_by(src) %>% 
  summarise("mean" = mean(bytes)) %>% filter(mean > 37543) %>% select(src)

dl6 |> collect()
```

    # A tibble: 1 × 1
      src        
      <chr>      
    1 13.46.35.35

Ответ: 13.46.35.35

#### 4. Обнаружение канала управления

Зачастую в корпоротивных сетях находятся ранее зараженные системы,
компрометация которых осталась незамеченной. Такие системы генерируют
небольшое количество трафика для связи с панелью управления бот-сети, но
с одинаковыми параметрами – в данном случае с одинаковым номером порта.
Какой номер порта используется бот-панелью для управления ботами?

``` r
d <- dbGetQuery(con, "SELECT min(bytes),max(bytes),max(bytes) - min(bytes), avg(bytes), port,count(port) FROM data group by port having avg(bytes) - min(bytes) < 10 and min(bytes) != max(bytes)")
d %>% select(port)
```

      port
    1  124

Ответ: port 124

#### 5. Обнаружение P2P трафика

Иногда компрометация сети проявляется в нехарактерном трафике между
хостами в локальной сети, который свидетельствует о горизонтальном
перемещении (lateral movement). В нашей сети замечена система, которая
ретранслирует по локальной сети полученные от панели управления бот-сети
команды, создав таким образом внутреннюю пиринговую сеть. Какой
уникальный порт используется этой бот сетью для внутреннего общения
между собой?

``` r
d <- dbGetQuery(con, "SELECT min(bytes),max(bytes),max(bytes) - min(bytes) as anomaly, avg(bytes), port,count(port) FROM data where (src LIKE '12.%' or src LIKE '13.%' or src LIKE '14.%') and (dst LIKE '12.%' or dst LIKE '13.%' or dst LIKE '14.%') group by port order by anomaly desc limit 1")
d %>% select(port)
```

      port
    1  115

Ответ: port 115

#### 6. Чемпион малвари

Нашу сеть только что внесли в списки спам-ферм. Один из хостов сети
получает множество команд от панели C&C, ретранслируя их внутри сети. В
обычных условиях причин для такого активного взаимодействия внутри сети
у данного хоста нет. Определите IP такого хоста.

#### 7. Скрытая бот-сеть

В нашем трафике есть еще одна бот-сеть, которая использует очень большой
интервал подключения к панели управления. Хосты этой продвинутой
бот-сети не входят в уже обнаруженную нами бот-сеть. Какой порт
используется продвинутой бот-сетью для коммуникации?

#### 8. Внутренний сканнер

Одна из наших машин сканирует внутреннюю сеть. Что это за система?

## Вывод

В результате работы удалось изучить возможности СУБД Clickhouse для
обработки и анализ больших данных, получить навыки применения Clickhouse
совместно с языком программирования R, получить навыки анализа
метаинфомации о сетевом трафике, получить навыки применения облачных
технологий хранения, подготовки и анализа данных: Managed Service for
ClickHouse, Rstudio Server.
