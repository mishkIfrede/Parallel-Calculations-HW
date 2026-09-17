public class trash {
    public static final int A = 10; //
    public static final int B = 20;
    public static final int N = 10; // вместо SIZE
    public static final double H = (B - A) / (double) N; //
    public static void main(String[] args) throws InterruptedException {
        var acc = 0;
        double x = A;
            for (int i = 0; i < N; i++) {
            x = A + i * H;
            double y = Math.sin(x*x) - 2;
            acc += y;
            }
        System.out.println(acc);
    }
}
