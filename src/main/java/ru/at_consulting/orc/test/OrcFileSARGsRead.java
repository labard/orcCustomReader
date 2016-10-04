package ru.at_consulting.orc.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;


/**
 * Простой загрузчик из Orc. Использует внутри себя проброс через интерфейс простого предиката и ограничения набора полей.
 * Тестировался на простой сущности из 3 полей (int,long,string)
 * На версии hive exec 1.2.1 не работал из-за отсутсвия минимальных и максимальных значений в описании колонки ColumnStatistics,
 * предположительно из-за проблем каста к конкретному типу колонки. В новой версии проблема решена явным указанием типа поля в аргументы.
 * Created by DAIvanov on 03.10.2016.
 */
public class OrcFileSARGsRead {

    public static void main(String[] args) throws IOException {
        //url с конфигурацией hdp
        final URL configUrl = new URL(args[0]);
        //путь к папке внутри файловой системы hdp
        final String dirPath = args[1];
        URI uri = getFileSystemUri(configUrl);

        Path path0 = new Path(uri + dirPath);
        FileSystem fs = path0.getFileSystem(new Configuration());
        final Reader reader = OrcFile.createReader(fs, path0);
        final StructObjectInspector objectInspector = (StructObjectInspector) reader.getObjectInspector();

        //описание структуры orc файла
        final List<? extends StructField> allStructFieldRefs = objectInspector.getAllStructFieldRefs();

        //учитывает дополнительное поле со структурой данных, хранящееся в orc файле
        //нужно при использовании include и searchArgument
        final int fakeplusNcolumns = allStructFieldRefs.size() + 1;
        String[] names = new String[fakeplusNcolumns];
        //приписываем любое значение фэйковому поля для сохранения правильных индексов остальных полей при накладывании ограничений на них
        //нужно только при использовании searchArgument
        names[0] = "fakeString";
        final int[] count = {1};
        allStructFieldRefs.forEach((Consumer<StructField>) structField -> {
            names[count[0]] = structField.getFieldName();
            count[0]++;
        });
        //создаём из общего интерфейса конкретный читатель по нашим ограничениям
        final RecordReader customReader = reader.rowsOptions(getOptions(fakeplusNcolumns, names));
        OrcStruct row = null;
        while (customReader.hasNext()) {
            row = (OrcStruct) customReader.next(row);
            System.out.println(row);
        }

    }

    private static URI getFileSystemUri(URL configUrl) {
        Configuration conf = new Configuration();
        FileSystem fileSystem;
        conf.addResource(configUrl);
        try {
            fileSystem = FileSystem.get(conf);

        } catch (IOException e) {
            throw new IllegalStateException("Couldn't get filesystem", e);
        }
        return fileSystem.getUri();
    }

    public static Reader.Options getOptions(int nColumns, String[] names) {
        final Reader.Options options = new Reader.Options();
        //массив должен включать в себя весь список полей, 0 поле - дополнительное поле со структурой,
        //true или false в нём для пользователя не имеет значения
        //при перечислении не всех полей внутри hive возникает ArrayIndexOutOfBound
        final boolean[] included = new boolean[nColumns];
        Arrays.fill(included, false);
        included[0] = true;
        included[1] = true;
        included[2] = false;
        included[3] = true;
        //поле для которого выполняется предикат должно входить в include с true, в противном случае выпадет java.lang.AssertionError: Index is not populated for ...
        //вероятно проверка предиката выполняется после отброса include
        final SearchArgument argument = SearchArgumentFactory.newBuilder().startAnd().lessThan(names[3], PredicateLeaf.Type.STRING, "a").end().build();
        System.out.println("argument = " + argument);
        options.include(included);
        //в список полей (второй аргумент метода) должны входить все поля сущности, включая фейковое, для определения верного порядкового номера поля, по которому
        //должен выполняться предикат.
        //в коде сделана попытка решить вопрос с нулевым полем со структурой RecordReaderImpl.mapSargColumnsToOrcInternalColIdx(),
        //но что-то она не работает, отсюда необходимость первого фэйкового элемента
        //в случае отсутствия фейкового элемента пересатё работать отсеивание по предикату, так как после выполнения конструктора RecordReaderImpl.SargApplier
        //весь массив sargColumns сожержит значение false, как следствие, предикат не выполняется ни для одной колонки
        options.searchArgument(argument, names);
        return options;
    }
}
