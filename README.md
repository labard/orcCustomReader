Простой загрузчик из Orc. Использует внутри себя проброс через интерфейс простого предиката и ограничение набора возвращаемых полей.
 * Тестировался на простой сущности из 3 полей (int,long,string)
 * На версии hive exec 1.2.1 не работал из-за отсутсвия минимальных и максимальных значений в описании колонки ColumnStatistics,
 * предположительно из-за проблем каста к конкретному типу колонки. В новой версии проблема решена явным указанием типа поля в аргументах.