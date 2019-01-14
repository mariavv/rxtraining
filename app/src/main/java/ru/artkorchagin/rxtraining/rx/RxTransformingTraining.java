package ru.artkorchagin.rxtraining.rx;

import org.reactivestreams.Publisher;

import java.util.Arrays;
import java.util.List;

import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.Single;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.BooleanSupplier;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import io.reactivex.observables.GroupedObservable;
import ru.artkorchagin.rxtraining.entity.Entity;
import ru.artkorchagin.rxtraining.exceptions.NotImplementedException;

/**
 * @author Arthur Korchagin (artur.korchagin@simbirsoft.com)
 * @since 13.11.18
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class RxTransformingTraining {

    /* Тренировочные методы */

    /**
     * Преобразование чисел в строки
     *
     * @param intObservable - источник
     * @return {@link Observable<String>} - который эммитит строки,
     * преобразованные из чисел в {@code intObservable}
     */
    public Observable<String> transformIntToString(Observable<Integer> intObservable) {
        return intObservable.map(String::valueOf);
    }

    /**
     * Преобразование {@link Observable<Integer>} эммитящих идентификаторы сущностей в сами
     * сущности, которые должны быть получены с помощью метода {@link #requestApiEntity(int)}
     *
     * @param idObservable - идентификаторы сущностей
     * @return {@link Observable<Entity>} эммитит сущности, соответствующие идентификаторам из
     * {@code idObservable}
     */
    public Observable<Entity> requestEntityById(Observable<Integer> idObservable) {
        return idObservable.map(Entity::new);
    }

    /**
     * Распределение имён из {@code namesObservable} по первой букве имени, в отдельные
     * {@link GroupedObservable}
     *
     * @param namesObservable - {@link Observable<String>} с именами
     * @return {@link Observable} который эммитит {@link GroupedObservable} - сгруппированный
     * поток имён объединённых первой буквой в имени
     */
    public Observable<GroupedObservable<Character, String>> distributeNamesByFirstLetter(Observable<String> namesObservable) {
        return namesObservable.groupBy(v -> v.charAt(0));
    }

    /**
     * Объединить элементы, полученные из {@code intObservable} в списки {@link List} с максимальным
     * размером {@code listsSize}
     *
     * @param listsSize     максимальный размер списка элементов
     * @param intObservable {@link Observable} с произвольным количеством рандомных чисел
     * @return {@code Observable} который эммитит списки чисел из {@code intObservable}
     */
    public Observable<List<Integer>> collectsIntsToLists(int listsSize, Observable<Integer> intObservable) {
        return intObservable.buffer(listsSize);
    }

    /* Вспомогательные методы */

    /**
     * Выполнение HTTP запроса и эммит полученной сущности, соответствующей заданному идентификатору
     * (Вспомогательный метод! Не изменять!)
     *
     * @param id - Идентификатор сущности {@link Entity}
     * @return {@link Observable<Entity>} который эммитит полученную сущность
     */
    Observable<Entity> requestApiEntity(int id) {
        // Выполнение запроса и эммит сущности
        return Observable.just(new Entity(id));
    }

}
