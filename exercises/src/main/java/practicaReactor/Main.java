package practicaReactor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class Main {
    public static void main (String[] arg) throws InterruptedException {
        //doOnNext - Util usualmente para depuración
        //subscribe - Recolección del dato final tras procesos previos cuando concatenaste diversos operadores.
        List<Persona> personas = List.of(new Persona(1,"jose",22),
                new Persona(2,"jorge",23),
                new Persona(3,"pedro",33));
        Persona persona = new  Persona(10,"ana",29);
        Mono.just(personas)
//                .doOnNext(System.out::println)
                .subscribe(System.out::println);
        System.out.println("============================");
        Flux.fromIterable(personas).subscribe(System.out::println);
        System.out.println("============================");
        //Collect flux en una lista
        Flux<Persona> personaFlux = Flux.fromIterable(personas);
        personaFlux.collectList().subscribe(lista -> System.out.println(lista));

        //creacion de flujos vacios con empty
        Mono.empty();
        Flux.empty();
        System.out.println("Range============================");
        //creacion de flujos de rango para flux
        Flux.range(0,3).doOnNext(System.out::println).subscribe(); //de 0 a 2;

        System.out.println("Repeat============================");
        //Repetir flujos de datos
        Flux.fromIterable(personas).repeat(2).subscribe(System.out::println);
        Mono.just(persona).repeat(2).subscribe(System.out::println);

        System.out.println("MAP============================");
        //METODO MAP
        Flux.fromIterable(personas).map(p->{
            p.setEdad(p.getEdad()+10);
            return p;
        }).subscribe(System.out::println);

        System.out.println("MAP============================");
        //METODO MAP
        Flux.fromIterable(personas).map(p->{
            p.setEdad(p.getEdad()+10);
            return p;
        }).subscribe(System.out::println);

        //METODO MAP
        //El siguiente codigo no aumenta el 10 en la impresion, esto debido a que el Map retorna el flujo cambiado, recuerda la INMUTABILIDAD
        Flux<Integer> fx = Flux.range(0,3);
        fx.map(x->x+10);
        fx.subscribe(System.out::println);
        //lo que se debe hacer es guardar el flujo
        Flux<Integer>fxAdd10 = fx.map(x->x+10);
        fxAdd10.subscribe(System.out::println);

        //el FLATMAP pide como retorno otro flujo de datos
        System.out.println("FLATMAP============================");
        personas = List.of(new Persona(1,"jose",22),
                new Persona(2,"jorge",23),
                new Persona(3,"pedro",33));
        Flux.fromIterable(personas)
                .flatMap(p->{
                    p.setEdad(p.getEdad()+10);
                    return Mono.just(p);
                }).subscribe(System.out::println);

        System.out.println("GROUP BY============================");
        personas = List.of(new Persona(1,"jose",22),
                new Persona(1,"jorge",23),
                new Persona(3,"pedro",33));
        //Se utiliza collectList debido a que el groupBy se retorna un GroupedFlux
        Flux.fromIterable(personas)
                .groupBy(Persona::getId)
                .flatMap(Flux::collectList)
                .subscribe(System.out::println);

        System.out.println("FILTER============================");
        personas = List.of(new Persona(1,"jose",22),
                new Persona(2,"jorge",23),
                new Persona(3,"pedro",33));
        //Se utiliza collectList debido a que el groupBy se retorna un GroupedFlux
        Flux.fromIterable(personas)
                .filter(p->p.getEdad()>30)
                .subscribe(System.out::println);

        System.out.println("DISTINCT============================");

        //el distinct en primitivos si funciona de forma directa
        Flux.fromIterable(List.of(1,2,2,1)).distinct().subscribe(System.out::println);

        //el distinct en objetos no funciona directamente asi se tengan los mismos datos
        //para el uso del DISTINCT la clase debe tener el metodo equals and hashCode, este sera usado para discernir
        personas = List.of(new Persona(1,"jose",22),
                new Persona(1,"jorge",23),
                new Persona(3,"pedro",33));
        //Se utiliza collectList debido a que el groupBy se retorna un GroupedFlux
        Flux.fromIterable(personas)
                .distinct()
                .subscribe(System.out::println);

        System.out.println("TAKE============================");
        personas = List.of(new Persona(1,"jose",22),
                new Persona(2,"jorge",23),
                new Persona(3,"pedro",33));
        //take tomo los n primeros
        Flux.fromIterable(personas)
                .take(1)
                .subscribe(System.out::println);
        //takeLast tomo los n ultimos
        Flux.fromIterable(personas)
                .takeLast(1)
                .subscribe(System.out::println);

        System.out.println("SKIP============================");
        personas = List.of(new Persona(1,"jose",22),
                new Persona(2,"jorge",23),
                new Persona(3,"pedro",33));
        //saltear n elementos primeros
        Flux.fromIterable(personas)
                .skip(1)
                .subscribe(System.out::println);
        //saltear n elementos ultimos
        Flux.fromIterable(personas)
                .skipLast(1)
                .subscribe(System.out::println);

        System.out.println("MERGE============================");
        personas = List.of(new Persona(1,"jose",22),
                new Persona(2,"jorge",23),
                new Persona(3,"pedro",33));
        List<Persona> personas2 = List.of(new Persona(4,"miguel",50),
                new Persona(5,"aida",58),
                new Persona(6,"pablo",61));
        List<Venta> ventas = List.of(new Venta(1, LocalDateTime.now()),new Venta(2,LocalDateTime.now()));

        Flux<Persona> flux1 = Flux.fromIterable(personas);
        Flux<Persona> flux2 = Flux.fromIterable(personas2);
        Flux<Venta> flux3 = Flux.fromIterable(ventas);

        //Merge sirve para mezclar flujos y devolverlo en un nuevo Flux, inclusive de distintas Clases devolviendo un Flux<Object>
        Flux.merge(flux1,flux2,flux3).subscribe(System.out::println);

        System.out.println("ZIP============================");

        //Zip espera que todos los flujos emitan un valor, y se termina cuando el flujo mas corto termine, en este caso el flujo3 con 2 datos
        Flux.zip(flux1,flux2,(p1,p2)->String.format("Flux1: %s, Flux2: %s",p1,p2)).subscribe(System.out::println);
        Flux.zip(flux1,flux2,flux3).subscribe(System.out::println);

        System.out.println("ZIPWITH============================");

        //ZipWith es una operacion que se hace directamente a un flujo, igual termina al terminar el flujo mas corto
        flux1.zipWith(flux3,(p1,p2)->String.format("Flux1: %s, Flux2: %s",p1,p2)).subscribe(System.out::println);

        System.out.println("ERROR============================");
        personas = List.of(new Persona(1,"jose",22),
                new Persona(2,"jorge",23),
                new Persona(3,"pedro",33));
        //Retry va a intentar realizar el flujo
//        Flux.fromIterable(personas).concatWith(Flux.error(new RuntimeException("ERROR 165   "))).retry(2)
//                .doOnNext(System.out::println).subscribe();

        System.out.println("onErrorReturn============================");
        Flux.fromIterable(personas).concatWith(Flux.error(new RuntimeException("ERROR 165   ")))
                .onErrorReturn(new Persona(0,"XYZ",999)).subscribe(System.out::println);

        //Se podria usar el e.getMessage para la excepcion
        System.out.println("onErrorResume============================");
        Flux.fromIterable(personas).concatWith(Flux.error(new RuntimeException("ERROR 165   ")))
                .onErrorResume(e->Mono.just(new Persona(0,"XYZ",999))).subscribe(System.out::println);

//        //Nos permite usar un error personalizado
//        System.out.println("onErrorMap============================");
//        Flux.fromIterable(personas).concatWith(Flux.error(new RuntimeException("ERROR 165   ")))
//                .onErrorMap(e->new InterruptedException(e.getMessage())).subscribe(System.out::println);

        //Devolver un flujo default al tener vacio de una servicio por ej, solo se ejecuta si es vacio
        System.out.println("defaultEmpty============================");
        Mono.empty().defaultIfEmpty(new Persona(0,"DEAFULT",999)).subscribe(System.out::println);
        Flux.empty().defaultIfEmpty(new Persona(0,"DEAFULT",999)).subscribe(System.out::println);

        //Toma elementos hasta verificar que se cumple el predicado, el ultimo valor leido que cumple tambien pasa por el flujo.
        System.out.println("takeUntil============================");
        Flux.fromIterable(personas).takeUntil(p->p.getEdad()>22).subscribe(System.out::println);

        //Timeout de consulta de cada elemento
//        System.out.println("timeOut============================");
//        Flux.fromIterable(personas).delayElements(Duration.ofSeconds(3))
//                .timeout(Duration.ofSeconds(2)).subscribe(System.out::println);
//
//        Thread.sleep(5000);

        System.out.println("Operadores Matematicos============================");
        Flux.fromIterable(personas).collect(Collectors.averagingInt(Persona::getEdad)).subscribe(System.out::println);
        Flux.fromIterable(personas).count().subscribe(System.out::println);
        Flux.fromIterable(personas)
                .collect(Collectors.minBy(Comparator.comparing(Persona::getEdad)))
                .subscribe(p-> System.out.println(p.get()));
        Flux.fromIterable(personas)
                .collect(Collectors.summingInt(Persona::getEdad))
                .subscribe(System.out::println);
        // Resumen que obtiene count, sum, min, avg, max
        Flux.fromIterable(personas)
                .collect(Collectors.summarizingInt(Persona::getEdad))
                .subscribe(System.out::println);

    }
}
