import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.*;
import java.awt.event.ActionListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.util.UUID;
import java.time.LocalDate;
import java.awt.Color;
import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.GridLayout;
import java.awt.BorderLayout;
import java.awt.Font;


    public class AddProductGUI {
        public final static String HO_QUEUE = "ho_queue";
        public final static String HO_DBNAME = "HO";
        public final String dbName;
        public AddProductGUI(String dbName) {
            this.dbName = dbName;
            JFrame f = new JFrame(dbName+"database");
            JTextField t1, t2, t3,  t5, t6, t7;
            f.setSize(600, 1200);
            JPanel p = new JPanel();
            p.setBorder(new EmptyBorder(10, 10, 10, 10));
            if (dbName=="HO"){p.setBackground(new Color(236, 226, 103));}
            else {p.setBackground(new Color(236, 126, 103));}
            p.setLayout(new GridLayout(9, 1, 15, 10));
            f.add(p, BorderLayout.CENTER);
            t1 = new JTextField("product name", 16);
            JLabel productLabel = new JLabel("Product Name");
            productLabel.setFont(new Font("Verdana", Font.BOLD, 16));
            p.add(productLabel);
            p.add(t1);
        
            t2 = new JTextField("1",8);
            JLabel qtyLabel = new JLabel("Quantity");
            qtyLabel.setFont(new Font("Verdana", Font.BOLD, 16));
            p.add(qtyLabel);
            p.add(t2);
        
            t3 = new JTextField("Sousse", 16);
            JLabel regionLabel = new JLabel("Region");
            regionLabel.setFont(new Font("Verdana", Font.BOLD, 16));
            p.add(regionLabel);
            p.add(t3);

        
            t5 = new JTextField("1206",8);
            JLabel costLabel = new JLabel("Cost");
            costLabel.setFont(new Font("Verdana", Font.BOLD, 16));
            p.add(costLabel);
            p.add(t5);
        
            t6 = new JTextField("6",8);
            JLabel amtLabel = new JLabel("Amount");
            amtLabel.setFont(new Font("Verdana", Font.BOLD, 16));
            p.add(amtLabel);
            p.add(t6);
        
            t7 = new JTextField("10",8);
            JLabel taxLabel = new JLabel("Tax");
            taxLabel.setFont(new Font("Verdana", Font.BOLD, 16));
            p.add(taxLabel);
            p.add(t7);

            JLabel label = new JLabel("");
            p.add(label);
        
            t5.addKeyListener(new KeyAdapter() {
                public void keyPressed(KeyEvent ke) {
                    String value = t5.getText();
                    int l = value.length();
                    if (ke.getKeyChar() >= '0' && ke.getKeyChar() <= '9' || ke.getKeyChar() == '.') {
                        t5.setEditable(true);
                        label.setFont(new Font("Verdana", Font.BOLD, 16));
                        label.setText("");
                    } else {
                        t5.setEditable(false);
                        label.setFont(new Font("Verdana", Font.BOLD, 16));
                        label.setText("* Enter only numeric digits(0-9) or decimal point for cost field");
                    }
                }
            });
        
            t6.addKeyListener(new KeyAdapter() {
                public void keyPressed(KeyEvent ke) {
                    String value = t6.getText();
                    int l = value.length();
                    if (ke.getKeyChar() >= '0' && ke.getKeyChar() <= '9' || ke.getKeyChar() == '.') {
                        t6.setEditable(true);
                        label.setFont(new Font("Verdana", Font.BOLD, 16));
                        label.setText("");
                    } else {
                        t6.setEditable(false);
                        label.setFont(new Font("Verdana", Font.BOLD, 16));
                        label.setText("* Enter only numeric digits(0-9) or decimal point for amount field");
                    }
                }
            });
        
            t7.addKeyListener(new KeyAdapter() {
                public void keyPressed(KeyEvent ke) {
                    String value = t7.getText();
                    int l = value.length();
                    if (ke.getKeyChar() >= '0' && ke.getKeyChar() <= '9' || ke.getKeyChar() == '.') {
                        t7.setEditable(true);
                        label.setFont(new Font("Verdana", Font.BOLD, 16));
                        label.setText("");
                    } else {
                        t7.setEditable(false);
                        label.setFont(new Font("Verdana", Font.BOLD, 16));
                        label.setText("* Enter only numeric digits(0-9) or decimal point for tax field");
                    }
                }
            });


            JButton addBtn=new JButton("Add product");
            addBtn.addActionListener(new ActionListener(){
                public void actionPerformed(ActionEvent e){
                    String product = t1.getText();
                    String region = t3.getText();
                    int qty;
                    try {
                        qty = Integer.parseInt(t2.getText());
                    } catch (NumberFormatException ex) {
                        qty = 0; // or some other default value
                    }

                    float cost = Float.parseFloat(t5.getText());
                    float amt = Float.parseFloat(t6.getText());
                    float tax = Float.parseFloat(t7.getText());
                    float total = (qty *  cost) + amt + (tax * (qty * cost));
                    Product p = new Product( product, qty, cost, amt, tax, total, region);
                    ManageData.sendToDB(p,dbName);
                    if (dbName!=HO_DBNAME){
                    ManageData.sendToDB(p,HO_DBNAME);}
                    JFrame f = new JFrame("retrieved data");
                    JPanel p0 = new JPanel();
                    String column[] = {"id","product","qty","cost","amt","tax","total","region"};
                    String[][] products = ManageData.getAllProducts(dbName);
                    label.setText("* db updated with product "+ product);
                    JOptionPane.showMessageDialog(null, "product "+ p.product +" added successfully!");
                }
            });
                JButton showBtn = new JButton("Show products");
        showBtn.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
                new VisualiseData(dbName);
            }
        });

        p.add(addBtn); p.add(showBtn);

        f.add(p);
        f.setSize(450,550);
        f.setVisible(true);
    }
}

