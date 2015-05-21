import rx.lang.scala.Observable

Observable.just(1, 2, 3, throw new Exception(), 4, 5, 6, 7).take(2) subscribe {
  x => println(x)
}