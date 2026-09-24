package edu.usp.cs324.client;

import edu.usp.cs324.api.Job;
import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.ExecutionException;

public final class ClientGui {
    private final ClientConnection connection;
    private final JFrame frame = new JFrame("DistriLab — CS324 client");
    private final JComboBox<Job.Type> type = new JComboBox<>(Job.Type.values());
    private final JTextArea input = new JTextArea("8,3,11,2,-7", 5, 50);
    private final DefaultTableModel rows = new DefaultTableModel(
            new String[]{"Job", "Operation", "Input", "Result / error"}, 0) {
        @Override public boolean isCellEditable(int row, int column) { return false; }
    };

    public ClientGui(String host, int port) {
        connection = new ClientConnection(host, port);
        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        JPanel controls = new JPanel(new FlowLayout(FlowLayout.LEFT));
        JButton load = new JButton("Load CSV");
        JButton submit = new JButton("Submit job");
        controls.add(new JLabel("Bootstrap: " + host + ":" + port));
        controls.add(type);
        controls.add(load);
        controls.add(submit);
        JPanel editor = new JPanel(new BorderLayout(8, 8));
        editor.setBorder(BorderFactory.createEmptyBorder(12, 12, 12, 12));
        editor.add(controls, BorderLayout.NORTH);
        editor.add(new JScrollPane(input), BorderLayout.CENTER);
        editor.add(new JLabel("MAX / PRIMECOUNT: comma-separated integers. PRIMESUM: start,end (inclusive)."),
                BorderLayout.SOUTH);
        JTable table = new JTable(rows);
        table.setRowHeight(26);
        frame.add(editor, BorderLayout.NORTH);
        frame.add(new JScrollPane(table), BorderLayout.CENTER);
        frame.add(new JLabel(" Submit multiple jobs while earlier jobs run. Double-click a result to view details."),
                BorderLayout.SOUTH);
        table.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override public void mouseClicked(java.awt.event.MouseEvent event) {
                int row = table.rowAtPoint(event.getPoint());
                if (event.getClickCount() == 2 && row >= 0) {
                    JTextArea details = new JTextArea(rows.getValueAt(row, 3).toString(), 10, 65);
                    details.setEditable(false);
                    details.setLineWrap(true);
                    details.setWrapStyleWord(true);
                    JOptionPane.showMessageDialog(frame, new JScrollPane(details), "Job result", JOptionPane.INFORMATION_MESSAGE);
                }
            }
        });
        load.addActionListener(event -> loadCsv());
        submit.addActionListener(event -> submit());
        frame.setSize(950, 560);
        frame.setLocationByPlatform(true);
    }

    private void loadCsv() {
        JFileChooser chooser = new JFileChooser();
        if (chooser.showOpenDialog(frame) != JFileChooser.APPROVE_OPTION) return;
        var file = chooser.getSelectedFile().toPath();
        new SwingWorker<String, Void>() {
            @Override protected String doInBackground() throws Exception {
                return Files.readString(file, StandardCharsets.UTF_8);
            }
            @Override protected void done() {
                try { input.setText(get()); }
                catch (Exception e) { JOptionPane.showMessageDialog(frame, describe(e), "CSV error", JOptionPane.ERROR_MESSAGE); }
            }
        }.execute();
    }

    private void submit() {
        String snapshot = input.getText();
        Job.Type operation = (Job.Type) type.getSelectedItem();
        int row = rows.getRowCount();
        rows.addRow(new Object[]{row + 1, operation, snapshot.replaceAll("\\R", " "), "Running…"});
        new SwingWorker<String, Void>() {
            @Override protected String doInBackground() throws Exception {
                return connection.submit(JobInput.parse(operation, snapshot)).toString();
            }
            @Override protected void done() {
                try { rows.setValueAt(get(), row, 3); }
                catch (Exception e) { rows.setValueAt("ERROR: " + describe(e), row, 3); }
            }
        }.execute();
    }

    private static String describe(Exception error) {
        Throwable cause = error instanceof ExecutionException ? error.getCause() : error;
        return cause.getMessage() == null ? cause.toString() : cause.getMessage();
    }

    public static void main(String[] args) {
        if (args.length != 2) throw new IllegalArgumentException("ClientGui bootstrapHost bootstrapPort");
        int port = Integer.parseInt(args[1]);
        SwingUtilities.invokeLater(() -> new ClientGui(args[0], port).frame.setVisible(true));
    }
}
