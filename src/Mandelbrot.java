/*"Нарисовать" множество Мандельброта. Рассчитать для каждого пикселя на экране, входит он в это множество или нет.
Вычисления производятся на нескольких потоках. Вывод картинки - опционально.*/

import java.awt.image.BufferedImage; // работа с изображением
import javax.imageio.ImageIO; // сохранение png-файла (в той же папке)
import java.io.File; // работа с файлом
import java.io.IOException; // работа с файлом
import java.util.concurrent.ExecutorService; // пул потоков
import java.util.concurrent.Executors; // пул потоков

public class Mandelbrot {

    //  присвоение ARGB-цвета на основе оставшихся итераций
    private static int colorFromIter(int iter, int maxIter) {
        if (iter >= maxIter) { // точка принадлежит множеству или находится далеко за его границами
            return 0xFF000000;
        }
        float t = (float) iter / maxIter; // иначе - норминрование (для использования в цвете)
        // формулы цветов
        int r = (int) (9 * (1 - t) * t * t * t * 255);
        int g = (int) (15 * (1 - t) * (1 - t) * t * t * 255);
        int b = (int) (8.5f * (1 - t) * (1 - t) * (1 - t) * t * 255);
        return 0xFF000000 | (r << 16) | (g << 8) | b; // возвращаем цвет
    }

    private static int mandelbrotPixel(double x0, double y0, int maxIter) { // вычисляем пиксель по формуле c = x + iy
        double x = 0.0;
        double y = 0.0;
        int iter = 0;
        while (x * x + y * y <= 4.0 && iter < maxIter) { // условия выхода - квадрат модуля превышает 4 или превышено кол-во итераций
            double xtemp = x * x - y * y + x0; // расчёт текущей точки
            y = 2.0 * x * y + y0;
            x = xtemp;
            iter++;
        }
        return iter; // возвращаем кол-во итераций до выхода или maxIter
    }

    private static class MandelbrotTask implements Runnable { // назначение задачи для потока
        private final BufferedImage image;
        private final int width; // размеры изображения
        private final int height;
        private final int yStart; // обрабатываемый диапазон строк
        private final int yEnd;
        private final double xmin; // диапазон комплексной плоскости, видной на экране
        private final double xmax;
        private final double ymin;
        private final double ymax;
        private final int maxIter;

        MandelbrotTask(BufferedImage image, int width, int height, int yStart, int yEnd,
                       double xmin, double xmax, double ymin, double ymax, int maxIter) {
            this.image = image;
            this.width = width;
            this.height = height;
            this.yStart = yStart;
            this.yEnd = yEnd;
            this.xmin = xmin;
            this.xmax = xmax;
            this.ymin = ymin;
            this.ymax = ymax;
            this.maxIter = maxIter;
        }

        @Override
        public void run() {
            for (int y = yStart; y < yEnd; y++) { // проходим по всему массиву строк
                double ny = ymin + (y / (double) (height - 1)) * (ymax - ymin); // вычисляем у на комплекной оси
                for (int x = 0; x < width; x++) {
                    double nx = xmin + (x / (double) (width - 1)) * (xmax - xmin); // вычисляем х на реальной оси
                    int iter = mandelbrotPixel(nx, ny, maxIter); // вычисляем кол-во итераций
                    int color = colorFromIter(iter, maxIter); // вычисляем цвет пикселя
                    image.setRGB(x, y, color); // "закрашиваем" пиксель на изображении
                }
            }
        }
    }

    public static void main(String[] args) {
        final int width = 1920; // размерность дисплея - у моего дисплея 1920х1080
        final int height = 1080;
        final double xmin = -2.5; // задаём диапазон значений по комплексной плоскости
        final double xmax = 1.0;
        final double ymin = -1.0;
        final double ymax = 1.0;
        final int maxIter = 1000; // задаём максимально допустимое кол-во итераций

        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);

        int threads = Math.max(1, Runtime.getRuntime().availableProcessors()); // кол-во потоков = число ядер
        ExecutorService executor = Executors.newFixedThreadPool(threads); // инициализируем пул потоков

        int rowsPerTask = 16; // деление картинки на пачки строк
        for (int y = 0; y < height; y += rowsPerTask) {
            int yEnd = Math.min(height, y + rowsPerTask);
            executor.execute(new MandelbrotTask(image, width, height, y, yEnd, // отправляем задачу в пул
                    xmin, xmax, ymin, ymax, maxIter));
        }

        executor.shutdown();
        while (!executor.isTerminated()) { // ожидание завершения всех задач
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                System.out.println("Ошибка!");
            }
        }

        try {
            ImageIO.write(image, "png", new File("mandelbrot.png")); // сохраняем картинку
            System.out.println("Иллюстрация множества Мандельброта сохранена в mandelbrot.png");
        } catch (IOException e) {
            System.err.println("Ошибка сохранения изображения: " + e.getMessage());
        }
    }
}
