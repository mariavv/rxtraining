package ru.artkorchagin.rxtraining.rx;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.reactivex.Observable;

/**
 * @author Arthur Korchagin (artur.korchagin@simbirsoft.com)
 * @since 15.11.18
 */
public class RxCombiningTraining {

    /* Тренировочные методы */

    /**
     * Суммирование элементов двух последовательностей.
     *
     * @param integerObservable1 {@link Observable} с произвольным количеством рандомных чисел
     * @param integerObservable2 {@link Observable} с произвольным количеством рандомных чисел
     * @return {@link Observable} который эммитит числа, где i-й элемент равен сумме i-го элемента
     * {@code integerObservable1} и i-го элемента {@code integerObservable2}. Если в одной из
     * входящих последовательностей сработает  {@code onComplete или  {@code onError} то и в
     * результирующей последовательности тоже сработает этот метод.
     */
    public Observable<Integer> summation(Observable<Integer> integerObservable1, Observable<Integer> integerObservable2) {
        return integerObservable1.zipWith(integerObservable2, (int1, int2) -> int1 + int2);
    }

    /**
     * Поиск элементов по выбранной строке и категории
     *
     * @param searchObservable   Последовательность поисковых строк (в приложении может быть
     *                           введёнными строками в поисковую строку)
     * @param categoryObservable Последовательность категорий, которые необходимо отобразить
     * @return {@link Observable}  который эммитит списки элементов, с учётом поисковой строки из
     * {@code searchObservable} и выбранной категории из {@code categoryObservable};
     */
    public Observable<List<String>> requestItems(Observable<String> searchObservable,
                                                 Observable<Integer> categoryObservable) {
        return Observable.combineLatest(searchObservable, categoryObservable, (s, integer) -> {
            return searchItems(s, integer);
        });
    }

    /**
     * Композиция потоков, обращение с несколькими объектами {@link Observable}, как с одним.
     *
     * @param intObservable1 {@link Observable} с произвольным количеством рандомных чисел
     * @param intObservable2 {@link Observable} с произвольным количеством рандомных чисел
     * @return {@link Observable} который эммитит элементы из {@code intObservable1} и
     * {@code intObservable2}
     */
    public Observable<Integer> composition(Observable<Integer> intObservable1,
                                           Observable<Integer> intObservable2) {
        return intObservable1.mergeWith(intObservable2);
    }

    /**
     * Дополнительный элемент перед всеми элементами потока
     *
     * @param firstItem     Первый элемент, который необходимо добавить
     * @param intObservable {@link Observable} с произвольным количеством рандомных чисел
     * @return {@link Observable} который сначала эммитит элемент {@code firstItem}, а потом все
     * элементы последовательности {@code intObservable}
     */
    public Observable<Integer> additionalFirstItem(int firstItem, Observable<Integer> intObservable) {
        return intObservable.startWith(firstItem);
    }

    /* Вспомогательные методы */

    /**
     * Поиск по строкам и вывод
     *
     * @param searchString Строка поиска
     * @param categoryId   Категория
     * @return вывод некоторых элементов с учётом поисковой строки и выбранной категории
     */
    List<String> searchItems(String searchString, int categoryId) {
        ArrayList<String> list = new ArrayList<>();
        list.add(searchString + String.valueOf(categoryId));
        return Collections.emptyList();
    }

}