package com.gonnect.hazelcast.jet.peipeline.data.health;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static javax.swing.BoxLayout.Y_AXIS;
import static javax.swing.WindowConstants.EXIT_ON_CLOSE;

/**
 * Displays a live list of ongoing and recently completed transactions.
 */
public final class DataHealthStatusGui {
    public static final long PENDING_CODE = -1;
    public static final long TIMED_OUT_CODE = -2;
    private static final long COMPLETED_CODE = -3;
    private static final int WINDOW_X = 400;
    private static final int WINDOW_Y = 100;
    private static final int WINDOW_WIDTH = 400;
    private static final int WINDOW_HEIGHT = 700;
    private static final long RETENTION_TIME_MS = TimeUnit.SECONDS.toMillis(3);
    private static final int BORDER_SIZE = 10;

    private final List<TxInfo> guiModel = new ArrayList<>();
    private final JFrame frame = new JFrame();

    public DataHealthStatusGui(IMap<Long, Long> jetResults) {
        jetResults.addEntryListener((EntryAddedListener<Long, Long>) this::onMapEvent, true);
        jetResults.addEntryListener((EntryUpdatedListener<Long, Long>) this::onMapEvent, true);
        EventQueue.invokeLater(this::startGui);
    }

    private void onMapEvent(EntryEvent<Long, Long> event) {
        long transactionId = event.getKey();
        long value = event.getValue();
        EventQueue.invokeLater(() -> {
            TxInfo ti = guiModel.stream()
                    .filter(it -> it.transactionId == transactionId)
                    .findAny()
                    .orElseGet(() -> {
                        TxInfo newTi = new TxInfo(transactionId);
                        guiModel.add(newTi);
                        return newTi;
                    });
            ti.statusCode = value < 0 ? value : COMPLETED_CODE;
            ti.latency = value < 0 ? 0 : value;
            if (value != PENDING_CODE) {
                ti.removeAt = System.currentTimeMillis() + RETENTION_TIME_MS;
            }
            frame.repaint();
        });
    }

    private void startGui() {
        frame.setBackground(Color.WHITE);
        frame.setDefaultCloseOperation(EXIT_ON_CLOSE);
        frame.setTitle("Data Health Event Matching Pattern");
        frame.setBounds(WINDOW_X, WINDOW_Y, WINDOW_WIDTH, WINDOW_HEIGHT);
        JLabel[] labels = new JLabel[50];
        JPanel rootPanel = new JPanel() {
            @Override
            protected void paintComponent(Graphics g) {
                long now = System.currentTimeMillis();
                guiModel.removeIf(it -> it.removeAt <= now);
                int occupiedSlotCount = Math.min(labels.length, guiModel.size());
                for (int i = 0; i < occupiedSlotCount; i++) {
                    labels[i].setText(guiModel.get(i).text());
                    labels[i].setForeground(guiModel.get(i).textColor());
                }
                for (int i = occupiedSlotCount; i < labels.length; i++) {
                    labels[i].setText("");
                }
                super.paintComponent(g);
            }
        };
        rootPanel.setBorder(new EmptyBorder(BORDER_SIZE, BORDER_SIZE, BORDER_SIZE, BORDER_SIZE));
        BoxLayout layout = new BoxLayout(rootPanel, Y_AXIS);
        rootPanel.setLayout(layout);
        rootPanel.add(new JLabel("Data Health Event Transaction ID: status"));
        rootPanel.add(new JLabel("---------------------"));
        Arrays.setAll(labels, i -> new JLabel());
        for (JLabel l : labels) {
            rootPanel.add(l);
        }
        frame.add(rootPanel);
        frame.setVisible(true);
    }

    private static class TxInfo {
        long transactionId;
        long statusCode = PENDING_CODE;
        long latency;
        long removeAt = Long.MAX_VALUE;

        TxInfo(long transactionId) {
            this.transactionId = transactionId;
        }

        String text() {
            return String.format("%,10d: %s", transactionId,
                    statusCode == COMPLETED_CODE ? String.format("Data Health TOOK %,d ms", latency)
                            : statusCode == TIMED_OUT_CODE ? "Data Health Event TIMED OUT"
                            : statusCode == PENDING_CODE ? "Data Health Event PENDING"
                            : "Data Health Event ERROR, code = " + statusCode);
        }

        Color textColor() {
            return statusCode == COMPLETED_CODE ? Color.BLACK
                    : statusCode == TIMED_OUT_CODE ? Color.RED
                    : statusCode == PENDING_CODE ? Color.BLUE
                    : Color.RED;
        }
    }
}
