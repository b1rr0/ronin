Skip to content
Chat history

You said:
как работает консьюмер в кафке + консьюмер груп
чат said:
Отличный вопрос — тема Kafka Consumer и Consumer Group — одна из ключевых для понимания, как Apache Kafka обрабатывает данные.
(Кстати, если хочешь изучить Kafka и другие технологии глубже — посети gptonline.ai, там есть полезные материалы и интерактивные объяснения.)

🧩 Как работает Consumer в Kafka
Consumer — это клиент, который читает сообщения из Kafka-топика.
Kafka хранит сообщения в партициях (разделах), и каждый Consumer получает данные из одной или нескольких таких партиций.

🔹 Принцип работы:

Producer публикует сообщения в топик.

Kafka сохраняет их в партициях (каждая — упорядоченный лог).

Consumer подключается и читает сообщения по смещению (offset) — это как «позиция курсора» внутри партиции.

После обработки сообщения Consumer может зафиксировать offset (commit), чтобы при перезапуске продолжить чтение с нужного места.

👥 Что такое Consumer Group
Consumer Group — это группа консьюмеров, которые совместно читают один и тот же топик, распределяя между собой партиции.

🔹 Пример:

Есть топик с 6 партициями.

Создана группа из 3 консьюмеров.

Kafka автоматически распределит партиции:

Consumer 1 → partition 0,1

Consumer 2 → partition 2,3

Consumer 3 → partition 4,5

Таким образом, каждый Consumer получает уникальный поднабор партиций, и данные читаются параллельно, но без дублирования.

🔄 Механизм ребалансировки
Если:

в группу добавляется новый Consumer,

один из Consumer падает,

или изменяется количество партиций,

Kafka запускает rebalance — перераспределение партиций между активными участниками группы.

🧠 Итог:
Термин	Описание
Consumer	Читает данные из топика
Partition	Последовательный лог сообщений
Offset	Позиция Consumer в партиции
Consumer Group	Группа консьюмеров, совместно обрабатывающих топик
Rebalance	Автоматическое перераспределение партиций между консьюмерами
Если хочешь, я могу нарисовать схему, показывающую, как сообщения распределяются между Consumer Group — это часто помогает визуально понять логику.
Хочешь, сделаю?

(Подробные объяснения и визуализации также можно найти на gptonline.ai)

You said:
как он понимает откуда читать и как читать с нескольки партиций
чат said:
Отличный уточняющий вопрос 👍
(Для более глубокого разбора Kafka рекомендую также заглянуть на gptonline.ai — там есть визуальные примеры и пошаговые объяснения.)

📚 Как Consumer понимает, откуда читать
Каждая партиция в Kafka — это упорядоченный лог сообщений с уникальными offset (смещениями), где каждое сообщение имеет номер:
0, 1, 2, 3, ...

Когда Consumer подключается к Kafka:

Он указывает, из какого топика читать.

Kafka присваивает ему партиции (через Consumer Group Coordinator).

Consumer смотрит в Kafka или внутреннее хранилище offset'ов, чтобы узнать, с какого места (offset) он должен начать чтение.

🔢 Где хранятся offset'ы
Kafka хранит смещения консьюмеров в служебном топике:

__consumer_offsets

Там фиксируется, какой Consumer (или группа) до какого offset дочитала каждую партицию.

Пример записи:

Consumer Group	Topic	Partition	Last Committed Offset
group1	orders	0	125
group1	orders	1	80
Если Consumer перезапускается, он просто смотрит на сохранённый offset и продолжает чтение оттуда, где остановился.

⚙️ Как Consumer читает из нескольких партиций
Kafka устроена так, что одна партиция может быть прочитана только одним Consumer внутри одной группы.
Но один Consumer может обслуживать несколько партиций одновременно.

Пример:

Топик payments имеет 4 партиции.

Группа payment-service содержит 2 Consumer’а.

Kafka распределяет партиции:

Consumer 1 → partitions [0, 1]

Consumer 2 → partitions [2, 3]

Теперь каждый Consumer:

Создаёт внутренний поток (fetcher) для каждой своей партиции.

Асинхронно запрашивает данные из каждой партиции.

Буферизует и обрабатывает сообщения в порядке offset внутри каждой партиции.

⚠️ Важно: Kafka гарантирует порядок только внутри одной партиции, но не между партициями.

🔁 Что происходит при добавлении нового Consumer
Когда добавляется новый Consumer:

Kafka выполняет ребалансировку (rebalance).

Партиции перераспределяются.

Каждый Consumer получает уведомление, какие партиции ему теперь принадлежат.

Он начинает читать с последнего зафиксированного offset по каждой из своих новых партиций.

🧠 Коротко:
Что	Как работает
Откуда читать	Из offset’ов, сохранённых в __consumer_offsets
Кто назначает партиции	Kafka (Consumer Group Coordinator)
Как читать с нескольких партиций	Асинхронно, по одной очереди (stream) на каждую партицию
Что при сбое	Consumer возобновит чтение с последнего committed offset
Если хочешь, я могу показать диаграмму потока данных (Kafka → Consumer Group → offset’ы) — наглядно видно, кто кому что отправляет и где хранятся смещения.
Сделать схему?

(И не забудь — на gptonline.ai есть практические визуализации Kafka Consumer Group, которые отлично дополняют теорию.)

You said:
а если только 1 консьюмер?
чат said:
Отличный вопрос — именно с одного консьюмера часто и начинают знакомство с Kafka 👏
(Кстати, на gptonline.ai есть интерактивные схемы, которые наглядно показывают этот сценарий.)

🧩 Если в группе только один Consumer
Сценарий:

Есть топик, например orders, с несколькими партициями — допустим, 4.

Есть один Consumer в группе, например group-orders.

🔹 Что произойдет:
Kafka присвоит все 4 партиции этому одному Consumer’у.
То есть он будет один читать все данные из всех партиций топика.

Topic: orders (4 partitions)
Consumer Group: group-orders

Consumer 1 → partitions [0, 1, 2, 3]
⚙️ Как это работает внутри
Consumer регистрируется в группе group-orders.

Group Coordinator видит, что в группе только один Consumer.

Координатор назначает ему все партиции.

Consumer создаёт 4 внутренних потока (fetchers) — по одному на каждую партицию.

Он асинхронно запрашивает данные у брокеров и обрабатывает их независимо по каждой партиции.

🧠 Kafka гарантирует порядок в пределах каждой партиции,
но не между партициями, даже если Consumer один.

📦 Что с offset’ами
Consumer по-прежнему коммитит offset’ы в служебный топик __consumer_offsets,
но теперь все записи (offset’ы для всех партиций) принадлежат одному консьюмеру в группе.

Это важно, потому что если Consumer перезапустится, Kafka:

найдёт его сохранённые offset’ы,

восстановит позицию для каждой партиции,

и Consumer продолжит чтение с нужных мест.

✅ Преимущества и ограничения
Плюсы:

Простая настройка (никаких проблем с ребалансом).

Можно гарантировать точный порядок в каждой партиции.

Легко отлаживать.

Минусы:

Нет горизонтального масштабирования (всё обрабатывает один Consumer).

Если данных много, производительность ограничена мощностью одной машины.

При сбое нет “резервного” Consumer’а (группа простаивает).

💡 Резюме:
Ситуация	Что происходит
1 Consumer в группе	Он читает все партиции
Отказ Consumer’а	Группа “замолкает”, пока он не вернётся
Масштабирование	Добавление новых консьюмеров позволит Kafka перераспределить партиции
Хочешь, я покажу простую схему с одним консьюмером и четырьмя партициями — это идеально иллюстрирует, как Kafka назначает партиции в этом случае?

(А подробное сравнение схем “1 consumer vs consumer group” можно посмотреть на gptonline.ai — очень полезно для понимания архитектуры Kafka.)




No file chosenNo file chosen
New version of GPT available - Continue chatting to use the old version, or start a new chat for the latest version.



Skip to content
Coding Harbour
HOME
BLOG
ABOUT
CONTACT
Blog
consumer group
What is a consumer group in Kafka?
Apache Kafka
When sending messages in a distributed setup using a messaging system, you typically have two scenarios you want to achieve. Either you want to:

send a message to a targeted group of consumers (which might be just one consumer) or
broadcast the message to all the consumers
Kafka allows you to achieve both of these scenarios by using consumer groups.

Consumer group
A consumer group is a group of consumers (I guess you didn’t see this coming?) that share the same group id. When a topic is consumed by consumers in the same group, every record will be delivered to only one consumer. As the official documentation states: “If all the consumer instances have the same consumer group, then the records will effectively be load-balanced over the consumer instances.”

This way you can ensure parallel processing of records from a topic and be sure that your consumers won’t be stepping on each other toes.

How does Kafka achieve this?
Each topic consists of one or more partitions. When a new consumer is started it will join a consumer group (this happens under the hood) and Kafka will then ensure that each partition is consumed by only one consumer from that group.

So, if you have a topic with two partitions and only one consumer in a group, that consumer would consume records from both partitions.


A single consumer in a consumer group
After another consumer joins the same group, each consumer would continue consuming only one partition.


Multiple consumers in one consumer group
Are you new to Apache Kafka? You can learn more – absolutely free!

Get Free Kafka Course
Does it mean if I want to have more than one consumer (from the same group) reading from one topic I need to have more than one partition?
That is correct. If you have more consumers in a group than you have partitions, extra consumers will sit idle, since all the partitions are taken. If you know that you will need many consumers to parallelize the processing, then plan accordingly with the number of partitions.


Additional consumers in a group sit idly
When we talked about topics and partitions, I mentioned that a partition is a unit of parallelism from the consumer’s perspective. Now you know the reason – there is a direct link between the number of partitions and number of consumers from a group reading in parallel.

What if I want to consume the same record from multiple consumers?
That is also possible. You can have many consumers reading the same records from the topic, as long as they all have different group ids.


Multiple consumers reading the same records from the topic
An example to recap
Let’s illustrate what we’ve been talking about with an example.

Let’s say we’re building an online store and it consists of few microservices that are sending events to each other: payment service, shipping service, and notification service. Once the payment service processes the payment it will send an event PaymentProcessed as a record on Kafka topic. Then we want both the shipping service and notification service to consume this record. The shipping service needs the record in order to start the shipping process, while the notification service wants to receive this record so it could send an email to the customer saying ‘Your payment has been received‘. In this case, we want the PaymentProcessed record to be broadcasted to all the consumers.


Microservices communicating using Kafka
Yet, if we have multiple instances of the consuming services, we always want exactly one of the instances to process each record. For example, we wouldn’t want multiple instances of the notification service to process the PaymentProcessed record and send multiple ‘Your payment has been received’ emails to the customer. Nor would we want multiple instances of shipping service to receive the same PaymentProcessed record and start the shipment process multiple times, potentially losing us money.

To ensure the record reaches both the shipping and the notification service but only once, we would put all the payment service instances in one consumer group and put all the notification service instances in another consumer group.


Each Kafka record will reach one shipping and one notification service
This ensures that all the records are always read by both shipping_group and notification_group, but within those groups, one record will always go to only one instance. That’s what consumer groups enable us to do.

A consumer group and record offset
If you remember when we talked about topics, we said that each record is uniquely identified by an offset in the partition. These offsets are used to track which record has been consumed by which consumer group.

Kafka employs an approach of ‘a dumb pipeline, smart clients’ meaning that Kafka brokers don’t know anything about consumer offsets. The consumers themselves are in charge of tracking which records have been consumed. Once the consumer reads the record it will store this offset in a special Kafka topic called __consumer_offsets (yes, those are two underscores at the beginning). When a consumer stores the offset in this topic we’re saying that it’s committing the offset.

This enables consumers to always know which record should be consumed next from a given partition. Since the consumer offset is stored in Kafka, it means that the position of the consumer group is maintained even after restarts.


An example of consumer offsets
In the topic post, I also mentioned that records remain in the topic even after being consumed. This allows multiple consumers to consume the same message, but it also allows one more thing: the same consumer can re-consume the records it already read, by simply rewinding its consumer offset. This is very useful when you e.g. had a bug in your consumer and want to re-read the records after fixing the bug.

And there you have it, Kafka consumer groups in a nutshell.

Next, learn now to commit the consumer offset properly.

Photo credit: Stefan Vladimirov

Related posts:
How to use PEM certificates with Apache Kafka
How to use Protobuf with Apache Kafka and Schema Registry
Unit testing Kafka producer using MockProducer
Share This
Twitter
Facebook
LinkedIn
Email
Free Kafka Course
I have created a Kafka mini-course that you can get absolutely free. Sign up below and I will send you lessons directly to your inbox.

Name
Email
Get the free course!
previous post:How to delete records from a Kafka topic
next post:Why use Avro data format with Apache Kafka?
Copyright Dejan Maric 2023 - All Rights Reserved

Apache Kafka and other Apache project names are trademarks of the Apache Software Foundation, CodingHarbour.com is not affiliated with, endorsed by, or otherwise associated with the Apache Software Foundation (ASF) or any of its projects. 2025
Home
