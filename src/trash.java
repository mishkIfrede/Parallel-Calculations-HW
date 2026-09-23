import java.util.concurrent.atomic.DoubleAdder;
public class trash {
    public static void main(String[] args) throws InterruptedException {
        var doubleAdder = new DoubleAdder();
        System.out.println(doubleAdder);
        doubleAdder.add(15.01);
        System.out.println(doubleAdder);
    }
}
