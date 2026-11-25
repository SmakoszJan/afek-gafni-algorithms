Repozytorium zawiera implementacje algorytmów A i C z artykułu oraz implementacje dwóch poprawek C unikających śmierci wszystkich kandydatów. Uruchomienie może skończyć się elekcją więcej, niż jednego kandydata, jednak na miarę naszej wiedzy nie skończy się śmiercią wszystkich.

Algorytmy należy uruchamiać komentą

```sh
go run <plik> [--bench]
```

gdzie `<plik>` jest jednym z: `a.go`, `c.go`, `double.go`, a przełącznik `--bench` uruchamia ciche, wielokrotne wykonanie na kolejnych wartościach n, po czym podaje statystyki uruchomień (nie działa dla `double.go`). Algorytmy są domyślnie uruchamiane z ustalonymi parametrami `n` oraz przełącznikiem `log` (mówiącym o tym, czy logować przebieg wykonania). Algorytm C ma też parametr `VARIANT` zadający, który jego wariant należy uruchomić. Parametry te należy zmieniać w plikach źródłowych, w funkcji `main` na samym końcu pliku. Nie zdążyliśmy zaimplementować lepszego interfejsu.

Uruchomienie C w wariancie `BASIC`, szczególnie dla wysokich `n`, może skończyć się śmiercią wszystkich kandydatów. Jest to wada algorytmu, nie implementacji, opisana dokładniej w raporcie. Objawi się to deadlockiem (wszystkie gorutyny śpią).

`double.go` uruchamia algorytm A w sposób, który zawsze doprowadzi do wielokrotnej elekcji.
