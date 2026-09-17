import java.util.concurrent.atomic.AtomicInteger;

public class Integral {
    public static final int A = 10; // границы интегрируемого интервала
    public static final int B = 20;
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
    public static Thread taskThread(int n, int[] schedule, int[] results){
        return new Thread(()-> {
            var start = schedule[n];
            var finish = schedule[n] + ITEMS_PER_THREAD;
            var acc = 0;
            for (int i = start; i < finish; i++) {
                double x = A + i * H;
                double y = func(x);
                acc += y;
            }
            results[n] = acc;
        });
    }
    public static Thread taskMonitorThread(int n, int[] schedule, Acc acc){
        return new Thread(()-> {
            var start = schedule[n];
            var finish = schedule[n] + ITEMS_PER_THREAD;
            for (int i = start; i < finish; i++) {
                double x = A + i * H;
                double y = func(x);
                acc.addToAcc(y); // захват монитора, только при изменении суммы (??)
            }
        });
    }
    public static Thread taskAtomicThread(int n, int[] schedule, AtomicInteger acc){
        return new Thread(()-> {
            var start = schedule[n];
            var finish = schedule[n] + ITEMS_PER_THREAD;
            for (int i = start; i < finish; i++) {
                double x = A + i * H;
                double y = func(x);
                acc.addAndGet((int)y); // блокировка также только при изменении суммы (??)
            }
        });
    }

    public static void measureP() throws InterruptedException {

        var threadsStart = new int[THREADS];

        var threads = new Thread[THREADS];

        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * ITEMS_PER_THREAD;
        }

        int[] results = new int[THREADS];

        var pStart = System.nanoTime();

        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskThread(i, threadsStart, results);
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
        System.out.println(pResult);
        System.out.println("Parallel time (ms)");
        System.out.println((double)(pFinish - pStart)/1000000);
    }

    public static void measureMon() throws InterruptedException {

        var threadsStart = new int[THREADS];

        var threads = new Thread[THREADS];

        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * ITEMS_PER_THREAD;
        }

        int[] results = new int[THREADS];

        var pStart = System.nanoTime();
        Acc acc = new Acc();
        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskMonitorThread(i, threadsStart, acc);
        }

        for (int i = 0; i < THREADS; i++)
            threads[i].start();

        for (int i = 0; i < THREADS; i++)
            threads[i].join();

        var pResult = acc.acc;
        var pFinish = System.nanoTime();
        System.out.println("Monitor result");
        System.out.println(pResult);
        System.out.println("Monitor time (ms)");
        System.out.println((double)(pFinish - pStart)/1000000);
    }

    public static void measureAtomic() throws InterruptedException {

        var threadsStart = new int[THREADS];

        var threads = new Thread[THREADS];

        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * ITEMS_PER_THREAD;
        }

        int[] results = new int[THREADS];

        var pStart = System.nanoTime();
        var acc = new AtomicInteger(0);
        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskAtomicThread(i, threadsStart, acc);
        }

        for (int i = 0; i < THREADS; i++)
            threads[i].start();

        for (int i = 0; i < THREADS; i++)
            threads[i].join();

        var pResult = acc.get();
        var pFinish = System.nanoTime();
        System.out.println("Atomic result");
        System.out.println(pResult);
        System.out.println("Atomic time (ms)");
        System.out.println((double)(pFinish - pStart)/1000000);
    }

    public static void main(String[] args) throws InterruptedException {

        var threadsStart = new int[THREADS];

        var threads = new Thread[THREADS];

        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * ITEMS_PER_THREAD;
        }

        var start = System.nanoTime();
        // обыкновенное вычисление
        var acc = 0;
        double x = A;
        for (int i = 0; i < N; i++) {
            x = A + i * H;
            double y = func(x);
            acc += y;
        }
        var finish = System.nanoTime();

        System.out.println("Sequential result");
        System.out.println(acc);
        System.out.println("Sequential time (ms)");
        System.out.println((double)(finish - start)/1000000);

        measureP();
        measureAtomic();
        measureMon();
    }
}
