# Pepeunit Backend

## [Репозиторий документации](https://git.pepemoss.com/pepe/pepeunit/pepeunit.git) Pepeunit

## Внешние зависимости
0. Подготовленная база `Postgresql`
1. Подготовленная база `Clickhouse`
1. Развёрнутый `EMQX MQTT Broker`
1. `Redis`
1. Запущенный `Backend`

## Основные этапы развёртывания
0. Установите пакеты при помощи команды:
   ```bash
   go mod download
   ```
1. Настройте `.env` файл по образцу `.env_example`
1. Запустите `Backend` приложение командой:
   ```bash
   go run main.go
   ```
1. Подробнее о `build` в `Dockerfile`


## Что произойдёт в момент запуска приложения ?
0. Установка соединения с `Redis`, для получения обновлений конфигурации через `stream`
1. Включение `gorutine` для периодического обновления конфигурации
1. Установка соединнеия с `EMQX MQTT Broker`
1. Подписка на топпик по паттерну `<BACKEND_DOMAIN>/+/pepeunit`
