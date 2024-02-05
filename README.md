# Разработка антифрод-системы (в рамках курса OTUS MLOps)

## Цель разработки

Антифрод-система разрабатывается для блокирования мошеннических транзакций 
при совершении онлайн-платежей.

## Основные ограничения и требования (верхнеуровневый взгляд)
- Бюджет заметно ограничен
- Разработка MVP и вывод в ПРОМ должны быть проведены в сжатые сроки
- Разработка должна быть конкурентноспособна в части предотвращения ущерба
- Система должна выберживать пиковые нагрузки
- Следует учесть возможный отток клиентов при избытке ложных срабатываний
- Собственных ресурсов на размещение системы нет
- Недопустима утечка данных клиентов из системы (в т. ч. при её разработке)

## Выбор бизнес-метрик (верхнеуровнеый)
- Объём сэкономленных денег клиентов
  - Число заблокированных мошеннических транзакций
  - Удельный размер ущерба от пропущенных мошеннических транзакций 
- Скорость обработки транзакций
  - Среднее и пиковое время от поступления транзакции на обработку до получения вердикта о том, мошенническая ли она
  - Среднее и пиковое время от отправки пользователем транзакции до получения пользователем сообщения об успешной транзакции
- Доля ложно-положительных сработок (ЛПС)
  - Величина оттока клиентов

## Выбор метрики машинного обучения и ограничения на неё
Полнота (recall) и (precision) точность являются хорошими кандидатами на первичное рассмотрение,
так как они хорошо связаны с бизнес-метриками: можно непосредственно оценивать число пропущенных транзакций,
ущерба и ЛПС через них.  
Ориентирование на полноту будет вести к росту ЛПС, то есть многие легитимные транзакции будут заблокированы.
Это напрямую увеличит величину оттока клиентов, хоть и приведёт к минимизации удельного ущерба.  
Ориентирование же на точность будет вести к пропуску мошеннических транзакций, что увеличит удельный
ущерб, хоть и снизит число ЛПС.  
Данная задача, вероятно, заметно несбалансированная, так как в выборке скорее всего будет низкая доля 
мошеннических транзакций по сравнению с обычными. Поэтому, например, доля правильных ответов (accuracy)
тут будет неуместна, так как правильное выявление легитимных транзакций (которых в разы больше)
позволит пропустить много мошеннических транзакций, не сильно ухудшив метрику.  
Метрика ROC-AUC была бы больше интересна, если бы целью было что-то вроде ранживания транзакций для  
проверки дежурными, так как скорее отражает правильность попарного расположения транзакций по вероятности 
быть мошенническими. К тому же она совсем слабо связана с бизнес-метриками (сложно сказать, сильно
ли изменение ROC-AUC, допустим, с 0.95 до 0.97 влияет на число заблокированных мошеннических транзакций).  
В нашей задаче заметно превалирует нулевой класс (легитимные транзакции), поэтому хорошими кандидатами
на метрику [будут](https://www.researchgate.net/publication/338351315_The_advantages_of_the_Matthews_correlation_coefficient_MCC_over_F1_score_and_accuracy_in_binary_classification_evaluation)
как мера F-бета, так и коэффициент корреляции Мэтьюса (MCC). Выберем F1-меру (бета = 1) как заметно более
интерпретируемую и частоиспользуемую метрику. К тому же при необходимости мы можем немного изменить
коэффициент бета, когда будет более ясна ценность ошибок первого и второго рода.

**Выбранная метрика: F1-мера (максимизируется).**

Достаточность достигнутого уровня метрики определяется несколькими условиями:
1. По условию, если более 5 % легитимных транзакций система будет блокировать, начнётся отток клиентов.
Характеристики оттока нам неизвестны, поэтому не будем его допускать в принципе, то есть будем иметь
ограничение FP <= 0,05 * N, где N -- число легитисных транзакций.  
2. По условию в среднем на каждые 100 транзакций надо пропускать не больше двух мошеннических,
то есть есть ограничение FN <= 0,02 * T, где T -- общее число транзакций. При этом общий ущерб клиентов
от пропущенных мошеннических транзакций не должен превышать 500 тыс. руб. в месяц. 
3. Поток транзакций:
   - средний составляет 50 транзакций в секунду
   - пиковый составляет 400 транзакций в секунду
