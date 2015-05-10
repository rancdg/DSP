import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.Date;
import javax.imageio.ImageIO;
import javax.swing.JFrame;
import javax.swing.JPanel;

public class TextOverlay extends JPanel
{

    public TextOverlay(String imageUrl)
        throws IOException
    {
        this.imageUrl = imageUrl;
        try
        {
            image = ImageIO.read(new URL(imageUrl));
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
        setPreferredSize(new Dimension(image.getWidth(), image.getHeight()));
        image = process(image);
    }

    private BufferedImage process(BufferedImage old)
        throws IOException
    {
        int w = old.getWidth();
        int h = old.getHeight();
        BufferedImage img = new BufferedImage(w, h, 2);
        Graphics2D g2d = img.createGraphics();
        g2d.drawImage(old, 0, 0, null);
        g2d.setPaint(Color.red);
        g2d.setFont(new Font("Serif", 1, 20));
        String s = (new StringBuilder("<liorh>,<benarnon>:<instance>")).append((new Date()).toString()).append(">.").toString();
        FontMetrics fm = g2d.getFontMetrics();
        int x = img.getWidth() - fm.stringWidth(s) - 5;
        int y = fm.getHeight();
        g2d.drawString(s, x, y);
        g2d.dispose();
        return img;
    }

    protected void paintComponent(Graphics g)
    {
        super.paintComponent(g);
        g.drawImage(image, 0, 0, null);
    }

    private void create()
        throws IOException
    {
        JFrame f = new JFrame();
        f.setDefaultCloseOperation(3);
        f.add(new TextOverlay(imageUrl));
        f.pack();
        f.setVisible(true);
    }

    public BufferedImage getBuffer()
    {
        return image;
    }

    public static String getInstanceOfWorker()
        throws IOException
    {
        URL instanceOfWorker = new URL("http://169.254.169.254/latest/meta-data/instance-id");
        java.nio.channels.ReadableByteChannel rbc = Channels.newChannel(instanceOfWorker.openStream());
        FileOutputStream fos = new FileOutputStream("instance.txt");
        fos.getChannel().transferFrom(rbc, 0L, 0x7fffffffL);
        String result = readFile("instance.txt");
        return result;
    }

    public static String readFile(String fileName)
        throws IOException
    {
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String s;
        StringBuilder sb = new StringBuilder();
        for(String line = br.readLine(); line != null; line = br.readLine())
        {
            sb.append(line);
            sb.append("\n");
        }

        s = sb.toString();
        br.close();
        return s;
        Exception exception;
        exception;
        br.close();
        throw exception;
    }

    private BufferedImage image;
    private String imageUrl;
}