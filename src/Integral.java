import java.util.concurrent.atomic.DoubleAdder;

public class Integral {
    public static final double A = 10; // границы интегрируемого интервала
    public static final double B = 20;
    public static final int N = 20; // количество разбиений (вместо SIZE)
    public static final double H = (B - A) / N; // шаг выполнения
    public static final int THREADS = 4;
    public static final int ITEMS_PER_THREAD = N/THREADS; // кол-во вычислений, выполняемое одним потоком
    static class Acc{ // класс-сумма(интеграл) для вычисления с помощью захвата монитора
        volatile double acc = 0;
        synchronized public void addToAcc(double y){
            acc += y;
        }
    }
    public static double func(double x){ // подынтегральная функция
        return Math.sin(x*x) - 2;
    }
    public static Thread taskThread(int n, int[] schedule, double[] items, double[] results){
        return new Thread(()-> {
            var start = schedule[n];
            var finish = schedule[n] + ITEMS_PER_THREAD;
            var acc = 0;
            for (int i = start; i < finish; i++) {
                acc += items[i];
            }
            results[n] = acc;
        });
    }

    public static Thread taskMonitorThread(int n, int[] schedule, double[] items, Acc acc){
        return new Thread(()-> {
            var start = schedule[n];
            var finish = schedule[n] + ITEMS_PER_THREAD;
            for (int i = start; i < finish; i++) {
                acc.addToAcc(items[i]); // захват монитора, только при изменении суммы
            }
        });
    }

    // Atomic-вариант реализован с помощью DoubleAdder
    public static Thread taskAtomicThread(int n, int[] schedule, double[] items, DoubleAdder acc){
        return new Thread(()-> {
            var start = schedule[n];
            var finish = schedule[n] + ITEMS_PER_THREAD;
            for (int i = start; i < finish; i++) {
                acc.add(items[i]); // блокировка также только при изменении суммы
            }
        });
    }

    public static void measureP(double[] items) throws InterruptedException {

        var threadsStart = new int[THREADS];

        var threads = new Thread[THREADS];

        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * ITEMS_PER_THREAD;
        }

        double[] results = new double[THREADS];

        var pStart = System.nanoTime();

        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskThread(i, threadsStart, items, results);
        }

        for (int i = 0; i < THREADS; i++)
            threads[i].start();

        for (int i = 0; i < THREADS; i++)
            threads[i].join();

        var pResult = 0;
        for (int i = 0; i < THREADS; i++) {
            pResult += results[i];
        }
        var pFinish = System.nanoTime();
        System.out.println("Parallel result");
        System.out.println(H * pResult);
        System.out.println("Parallel time (ms)");
        System.out.println((double)(pFinish - pStart)/1000000);
    }

    public static void measureMon(double[] items) throws InterruptedException {

        var threadsStart = new int[THREADS];

        var threads = new Thread[THREADS];

        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * ITEMS_PER_THREAD;
        }

        double[] results = new double[THREADS];

        var pStart = System.nanoTime();
        Acc acc = new Acc();
        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskMonitorThread(i, threadsStart, items, acc);
        }

        for (int i = 0; i < THREADS; i++)
            threads[i].start();

        for (int i = 0; i < THREADS; i++)
            threads[i].join();

        var pResult = acc.acc;
        var pFinish = System.nanoTime();
        System.out.println("Monitor result");
        System.out.println(H * pResult);
        System.out.println("Monitor time (ms)");
        System.out.println((double)(pFinish - pStart)/1000000);
    }

    public static void measureAtomic(double[] items) throws InterruptedException {

        var threadsStart = new int[THREADS];

        var threads = new Thread[THREADS];

        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * ITEMS_PER_THREAD;
        }

        double[] results = new double[THREADS];

        var pStart = System.nanoTime();
        var acc = new DoubleAdder();;
        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskAtomicThread(i, threadsStart, items, acc);
        }

        for (int i = 0; i < THREADS; i++)
            threads[i].start();

        for (int i = 0; i < THREADS; i++)
            threads[i].join();

        var pResult = acc.sum();
        var pFinish = System.nanoTime();
        System.out.println("Atomic result");
        System.out.println(H * pResult);
        System.out.println("Atomic time (ms)");
        System.out.println((double)(pFinish - pStart)/1000000);
    }

    public static void main(String[] args) throws InterruptedException {

        var threadsStart = new int[THREADS];

        var threads = new Thread[THREADS];

        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * ITEMS_PER_THREAD;
        }

        double[] ls = new double[N];
        for (int i = 0; i < N; i++) { // расчёт точек y
            double x = A + i * H;
            double y = func(x);
            ls[i] = y;
        }

        double[] results = new double[THREADS];

        var start = System.nanoTime();
        // обыкновенное вычисление
        var acc = 0;
        for (int i = 0; i < N; i++) {
            acc += ls[i];
        }
        var finish = System.nanoTime();

        System.out.println("Sequential result");
        System.out.println(H * acc);
        System.out.println("Sequential time (ms)");
        System.out.println((double)(finish - start)/1000000);

        measureP(ls);
        measureAtomic(ls);
        measureMon(ls);
    }
}
