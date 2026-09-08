use crate::rpc::proto;
use anyhow::Result;
use crossterm::{
    event::{Event, EventStream, KeyCode, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use num_format::{Locale, ToFormattedString};
use ratatui::{
    Frame, Terminal,
    layout::{Alignment, Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols::Marker,
    text::{Line, Span},
    widgets::{
        Axis, Block, Chart, Dataset, Gauge, GraphType, LegendPosition, Paragraph, RenderDirection,
        Sparkline,
    },
};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::Instant;
use tokio_stream::StreamExt as TokioStreamExt;

const HISTORY_LEN: usize = 120;
const CHART_INTERVAL_SECS: f64 = 1.0;

struct MonitorApp {
    // Latest snapshot for counters/gauges (updates every tick)
    current: Option<proto::StatsSnapshot>,

    // Baseline for chart rate computation (updates every ~1s)
    rate_base: Option<proto::StatsSnapshot>,
    rate_base_time: Option<Instant>,

    // Chart data: ring buffers of rates at 1s resolution
    read_throughput: VecDeque<f64>,
    write_throughput: VecDeque<f64>,
    read_iops: VecDeque<f64>,
    write_iops: VecDeque<f64>,

    // Sparkline data
    total_ops: VecDeque<u64>,
}

impl MonitorApp {
    fn new() -> Self {
        Self {
            current: None,
            rate_base: None,
            rate_base_time: None,
            read_throughput: VecDeque::with_capacity(HISTORY_LEN),
            write_throughput: VecDeque::with_capacity(HISTORY_LEN),
            read_iops: VecDeque::with_capacity(HISTORY_LEN),
            write_iops: VecDeque::with_capacity(HISTORY_LEN),
            total_ops: VecDeque::with_capacity(HISTORY_LEN),
        }
    }

    fn update(&mut self, snapshot: proto::StatsSnapshot) {
        let now = Instant::now();

        // Push chart data points every ~1 second
        if let (Some(base), Some(base_time)) = (&self.rate_base, &self.rate_base_time) {
            let elapsed = base_time.elapsed().as_secs_f64();
            if elapsed >= CHART_INTERVAL_SECS {
                let read_bps =
                    (snapshot.bytes_read.saturating_sub(base.bytes_read)) as f64 / elapsed;
                let write_bps =
                    (snapshot.bytes_written.saturating_sub(base.bytes_written)) as f64 / elapsed;
                let read_ops = (snapshot
                    .read_operations
                    .saturating_sub(base.read_operations)) as f64
                    / elapsed;
                let write_ops = (snapshot
                    .write_operations
                    .saturating_sub(base.write_operations)) as f64
                    / elapsed;
                let total = (snapshot
                    .total_operations
                    .saturating_sub(base.total_operations)) as f64
                    / elapsed;

                push_ring(&mut self.read_throughput, read_bps);
                push_ring(&mut self.write_throughput, write_bps);
                push_ring(&mut self.read_iops, read_ops);
                push_ring(&mut self.write_iops, write_ops);
                push_ring(&mut self.total_ops, total as u64);

                self.rate_base = Some(snapshot);
                self.rate_base_time = Some(now);
            }
        } else {
            self.rate_base = Some(snapshot);
            self.rate_base_time = Some(now);
        }

        self.current = Some(snapshot);
    }

    fn current_read_rate(&self) -> f64 {
        self.read_throughput.back().copied().unwrap_or(0.0)
    }

    fn current_write_rate(&self) -> f64 {
        self.write_throughput.back().copied().unwrap_or(0.0)
    }

    fn current_read_iops(&self) -> f64 {
        self.read_iops.back().copied().unwrap_or(0.0)
    }

    fn current_write_iops(&self) -> f64 {
        self.write_iops.back().copied().unwrap_or(0.0)
    }

    fn current_total_ops(&self) -> u64 {
        self.total_ops.back().copied().unwrap_or(0)
    }

    fn chart_data(ring: &VecDeque<f64>) -> Vec<(f64, f64)> {
        let len = ring.len();
        ring.iter()
            .enumerate()
            .map(|(i, &v)| (i as f64 - (len as f64 - 1.0), v))
            .collect()
    }

    fn max_y(data: &[(f64, f64)]) -> f64 {
        data.iter().map(|(_, y)| *y).fold(0.0f64, f64::max).max(1.0)
    }
}

fn push_ring<T>(ring: &mut VecDeque<T>, val: T) {
    if ring.len() >= HISTORY_LEN {
        ring.pop_front();
    }
    ring.push_back(val);
}

pub async fn run_monitor(config_path: PathBuf, interval_ms: u32) -> Result<()> {
    let client = super::connect_rpc_client(&config_path).await?;
    let mut stream = client.stream_stats(interval_ms).await?;

    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(std::io::stdout(), LeaveAlternateScreen);
        original_hook(info);
    }));

    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = ratatui::backend::CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = MonitorApp::new();
    let mut events = EventStream::new();

    let result: Result<()> = loop {
        terminal.draw(|f| ui(f, &app))?;

        tokio::select! {
            result = TokioStreamExt::next(&mut stream) => {
                match result {
                    Some(Ok(snapshot)) => app.update(snapshot),
                    Some(Err(e)) => break Err(anyhow::anyhow!(e)),
                    None => break Ok(()),
                }
            }
            Some(Ok(event)) = TokioStreamExt::next(&mut events) => {
                if let Event::Key(key) = event
                    && (key.code == KeyCode::Char('q')
                        || (key.code == KeyCode::Char('c')
                            && key.modifiers.contains(KeyModifiers::CONTROL)))
                {
                    break Ok(());
                }
            }
        }
    };

    // Restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    // Restore default panic hook
    let _ = std::panic::take_hook();

    result
}

fn ui(f: &mut Frame, app: &MonitorApp) {
    let outer = Layout::vertical([
        Constraint::Length(1), // title
        Constraint::Fill(1),   // throughput chart
        Constraint::Fill(1),   // IOPS chart
        Constraint::Length(3), // sparkline + storage row
        Constraint::Length(6), // segment space row
        Constraint::Length(5), // counters row
        Constraint::Length(3), // memory row
        Constraint::Length(1), // footer
    ])
    .split(f.area());

    let title = Paragraph::new("ZeroFS Monitor")
        .alignment(Alignment::Center)
        .style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        );
    f.render_widget(title, outer[0]);

    if app.current.is_none() {
        let waiting = Paragraph::new("Waiting for data...")
            .alignment(Alignment::Center)
            .style(Style::default().fg(Color::DarkGray));
        f.render_widget(waiting, outer[1]);
        return;
    }

    render_throughput_chart(f, app, outer[1]);
    render_iops_chart(f, app, outer[2]);

    // Sparkline + Storage row
    let row3 = Layout::horizontal([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(outer[3]);
    render_total_ops_sparkline(f, app, row3[0]);
    render_storage_gauge(f, app, row3[1]);

    // Segment space row
    render_segment_space(f, app, outer[4]);

    // Counters row
    let row5 = Layout::horizontal([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(outer[5]);
    render_operations(f, app, row5[0]);
    render_tombstone_cleanup_stats(f, app, row5[1]);

    // Memory row
    render_memory_stats(f, app, outer[6]);

    // Footer
    let footer = Paragraph::new("Press q to quit")
        .alignment(Alignment::Center)
        .style(Style::default().fg(Color::DarkGray));
    f.render_widget(footer, outer[7]);
}

fn render_throughput_chart(f: &mut Frame, app: &MonitorApp, area: Rect) {
    let read_data = MonitorApp::chart_data(&app.read_throughput);
    let write_data = MonitorApp::chart_data(&app.write_throughput);

    let max_y = MonitorApp::max_y(&read_data).max(MonitorApp::max_y(&write_data)) * 1.1;

    let read_label = format!(
        "Read  {}/s",
        format_bytes_human(app.current_read_rate() as u64)
    );
    let write_label = format!(
        "Write {}/s",
        format_bytes_human(app.current_write_rate() as u64)
    );

    let datasets = vec![
        Dataset::default()
            .name(read_label)
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::Green))
            .data(&read_data),
        Dataset::default()
            .name(write_label)
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::Blue))
            .data(&write_data),
    ];

    let x_min = -(HISTORY_LEN as f64 - 1.0);

    let chart = Chart::new(datasets)
        .block(
            Block::bordered()
                .title(" I/O Throughput ")
                .title_style(Style::default().fg(Color::Yellow).bold()),
        )
        .x_axis(
            Axis::default()
                .bounds([x_min, 0.0])
                .labels(vec![Line::from(""), Line::from("now")]),
        )
        .y_axis(Axis::default().bounds([0.0, max_y]).labels(vec![
            Line::from("0"),
            Line::from(format_bytes_short(max_y / 2.0)),
            Line::from(format_bytes_short(max_y)),
        ]))
        .legend_position(Some(LegendPosition::TopLeft))
        .hidden_legend_constraints((Constraint::Ratio(1, 1), Constraint::Ratio(1, 1)));

    f.render_widget(chart, area);
}

fn render_iops_chart(f: &mut Frame, app: &MonitorApp, area: Rect) {
    let read_data = MonitorApp::chart_data(&app.read_iops);
    let write_data = MonitorApp::chart_data(&app.write_iops);

    let max_y = MonitorApp::max_y(&read_data).max(MonitorApp::max_y(&write_data)) * 1.1;

    let read_label = format!("Read  {}/s", format_ops(app.current_read_iops()));
    let write_label = format!("Write {}/s", format_ops(app.current_write_iops()));

    let datasets = vec![
        Dataset::default()
            .name(read_label)
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::Green))
            .data(&read_data),
        Dataset::default()
            .name(write_label)
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::Blue))
            .data(&write_data),
    ];

    let x_min = -(HISTORY_LEN as f64 - 1.0);

    let chart = Chart::new(datasets)
        .block(
            Block::bordered()
                .title(" IOPS ")
                .title_style(Style::default().fg(Color::Yellow).bold()),
        )
        .x_axis(
            Axis::default()
                .bounds([x_min, 0.0])
                .labels(vec![Line::from(""), Line::from("now")]),
        )
        .y_axis(Axis::default().bounds([0.0, max_y]).labels(vec![
            Line::from("0"),
            Line::from(format_ops(max_y / 2.0)),
            Line::from(format_ops(max_y)),
        ]))
        .legend_position(Some(LegendPosition::TopLeft))
        .hidden_legend_constraints((Constraint::Ratio(1, 1), Constraint::Ratio(1, 1)));

    f.render_widget(chart, area);
}

fn render_total_ops_sparkline(f: &mut Frame, app: &MonitorApp, area: Rect) {
    // Sparkline only renders the first `width` items of the slice. Feed newest-first
    // and render right-to-left so the rightmost bar is the most recent sample and the
    // visible window is always the last `width` seconds.
    let data: Vec<u64> = app.total_ops.iter().rev().copied().collect();
    let title = format!(
        " Total Ops: {}/s ",
        format_ops(app.current_total_ops() as f64)
    );
    let sparkline = Sparkline::default()
        .block(
            Block::bordered()
                .title(title)
                .title_style(Style::default().fg(Color::Yellow).bold()),
        )
        .data(&data)
        .direction(RenderDirection::RightToLeft)
        .style(Style::default().fg(Color::Magenta));
    f.render_widget(sparkline, area);
}

fn render_storage_gauge(f: &mut Frame, app: &MonitorApp, area: Rect) {
    let (used, max) = match &app.current {
        Some(s) => (s.used_bytes, s.max_bytes),
        None => (0, 0),
    };
    let inode_label = app
        .current
        .as_ref()
        .map(|s| {
            format!(
                "{} inode{}",
                s.used_inodes.to_formatted_string(&Locale::en),
                plural_suffix(s.used_inodes)
            )
        })
        .unwrap_or_default();
    let ratio = if max > 0 {
        (used as f64 / max as f64).min(1.0)
    } else {
        0.0
    };
    let label = format!(
        "{} / {} ({})",
        format_bytes_human(used),
        format_bytes_human(max),
        inode_label
    );
    let color = if ratio > 0.9 {
        Color::Red
    } else if ratio > 0.75 {
        Color::Yellow
    } else {
        Color::Blue
    };
    let gauge = Gauge::default()
        .block(
            Block::bordered()
                .title(" Storage (Logical) ")
                .title_style(Style::default().fg(Color::Yellow).bold()),
        )
        .gauge_style(Style::default().fg(color))
        .ratio(ratio)
        .label(label);
    f.render_widget(gauge, area);
}

fn render_operations(f: &mut Frame, app: &MonitorApp, area: Rect) {
    let s = app.current.as_ref();
    let lines = vec![
        Line::from(vec![
            Span::styled("Files  ", Style::default().fg(Color::White)),
            Span::raw(format!(
                "C:{} D:{} R:{}",
                s.map(|s| s.files_created.to_formatted_string(&Locale::en))
                    .unwrap_or_default(),
                s.map(|s| s.files_deleted.to_formatted_string(&Locale::en))
                    .unwrap_or_default(),
                s.map(|s| s.files_renamed.to_formatted_string(&Locale::en))
                    .unwrap_or_default(),
            )),
        ]),
        Line::from(vec![
            Span::styled("Dirs   ", Style::default().fg(Color::White)),
            Span::raw(format!(
                "C:{} D:{} R:{}",
                s.map(|s| s.directories_created.to_formatted_string(&Locale::en))
                    .unwrap_or_default(),
                s.map(|s| s.directories_deleted.to_formatted_string(&Locale::en))
                    .unwrap_or_default(),
                s.map(|s| s.directories_renamed.to_formatted_string(&Locale::en))
                    .unwrap_or_default(),
            )),
        ]),
        Line::from(vec![
            Span::styled("Links  ", Style::default().fg(Color::White)),
            Span::raw(format!(
                "C:{} D:{} R:{}",
                s.map(|s| s.links_created.to_formatted_string(&Locale::en))
                    .unwrap_or_default(),
                s.map(|s| s.links_deleted.to_formatted_string(&Locale::en))
                    .unwrap_or_default(),
                s.map(|s| s.links_renamed.to_formatted_string(&Locale::en))
                    .unwrap_or_default(),
            )),
        ]),
    ];
    let para = Paragraph::new(lines).block(
        Block::bordered()
            .title(" Operations (since startup) ")
            .title_style(Style::default().fg(Color::Yellow).bold()),
    );
    f.render_widget(para, area);
}

fn render_tombstone_cleanup_stats(f: &mut Frame, app: &MonitorApp, area: Rect) {
    let s = app.current.as_ref();
    let tombstone_cleanup_runs = s
        .map(|s| {
            format!(
                "{} cleanup run{}",
                s.tombstone_cleanup_runs.to_formatted_string(&Locale::en),
                plural_suffix(s.tombstone_cleanup_runs)
            )
        })
        .unwrap_or_default();
    let lines = vec![
        Line::from(format!(
            "Tombstones: {} created / {} processed",
            s.map(|s| s.tombstones_created.to_formatted_string(&Locale::en))
                .unwrap_or_default(),
            s.map(|s| s.tombstones_processed.to_formatted_string(&Locale::en))
                .unwrap_or_default(),
        )),
        Line::from(format!(
            "Extents deleted: {} ({})",
            s.map(|s| s
                .tombstone_cleanup_extents_deleted
                .to_formatted_string(&Locale::en))
                .unwrap_or_default(),
            tombstone_cleanup_runs,
        )),
    ];
    let para = Paragraph::new(lines).block(
        Block::bordered()
            .title(" Tombstone Cleanup (since startup) ")
            .title_style(Style::default().fg(Color::Yellow).bold()),
    );
    f.render_widget(para, area);
}

fn render_segment_space(f: &mut Frame, app: &MonitorApp, area: Rect) {
    let block = Block::bordered()
        .title(" Segment Space ")
        .title_style(Style::default().fg(Color::Yellow).bold());

    let seg = app
        .current
        .as_ref()
        .expect("dashboard rendered without a current status")
        .segment_reclaim
        .as_ref()
        .expect("server omitted segment-reclamation statistics");

    let inner = block.inner(area);
    f.render_widget(block, area);
    let rows = Layout::vertical([
        Constraint::Length(1), // live / dead / awaiting bar
        Constraint::Length(1), // footprint totals
        Constraint::Length(1), // live executor resource use
        Constraint::Length(1), // deletion safety state or checkpoint block
    ])
    .split(inner);

    // Bar: live (gray) | dead trapped in live segments (red) | fully dead,
    // awaiting the delete horizon (yellow). The three sum to appended bytes.
    let appended = seg.appended_bytes.max(1);
    let awaiting = seg.awaiting_delete_bytes.min(seg.reclaimable_bytes);
    let dead_trapped = seg.reclaimable_bytes.saturating_sub(awaiting);
    let width = rows[0].width as u64;
    let live_w = (seg.live_bytes.saturating_mul(width) / appended) as usize;
    let dead_w = (dead_trapped.saturating_mul(width) / appended) as usize;
    let await_w = (awaiting.saturating_mul(width) / appended) as usize;
    // Flooring leftover goes to live. Live is a quiet light-shade track; only
    // the reclaimable bytes render as solid blocks, so a healthy store recedes
    // and dead space stands out.
    let slack = (width as usize).saturating_sub(live_w + dead_w + await_w);
    let bar = Line::from(vec![
        Span::styled("░".repeat(live_w + slack), Style::default().fg(Color::Gray)),
        Span::styled("█".repeat(dead_w), Style::default().fg(Color::Red)),
        Span::styled("█".repeat(await_w), Style::default().fg(Color::Yellow)),
    ]);
    f.render_widget(Paragraph::new(bar), rows[0]);

    // Numbers match the bar's three disjoint bands (live + dead + pending =
    // appended); the percentage is the whole reclaimable fraction (dead +
    // pending deletion), the one headline health figure.
    let dead_pct = seg
        .reclaimable_bytes
        .saturating_mul(100)
        .checked_div(appended)
        .unwrap_or(0);
    let mut totals = vec![
        Span::styled(
            format!(
                "{} segment{}",
                seg.segment_count.to_formatted_string(&Locale::en),
                plural_suffix(seg.segment_count)
            ),
            Style::default().fg(Color::White),
        ),
        Span::raw("   "),
    ];
    totals.push(Span::styled(
        format!("{} live", format_bytes_human(seg.live_bytes)),
        Style::default().fg(Color::Gray),
    ));
    if seg.unflushed_bytes > 0 {
        // Raw write-buffer bytes can include superseded frames, so display
        // them independently instead of subtracting them from live bytes.
        totals.push(Span::raw("   "));
        totals.push(Span::styled(
            format!("{} buffered", format_bytes_human(seg.unflushed_bytes)),
            Style::default().fg(Color::Cyan),
        ));
    }
    totals.push(Span::raw("   "));
    totals.push(Span::styled(
        format!("{} dead", format_bytes_human(dead_trapped)),
        Style::default().fg(Color::Red),
    ));
    // The fully-dead subset, shown only while it exists so a store that isn't
    // waiting never carries a puzzling "0 B" band.
    if awaiting > 0 {
        totals.push(Span::raw("   "));
        totals.push(Span::styled(
            format!("{} pending delete", format_bytes_human(awaiting)),
            Style::default().fg(Color::Yellow),
        ));
    }
    totals.push(Span::raw("   "));
    totals.push(Span::styled(
        format!("{}% reclaimable", dead_pct),
        Style::default().fg(Color::DarkGray),
    ));
    f.render_widget(Paragraph::new(Line::from(totals)), rows[1]);

    let executor = Line::from(vec![
        Span::styled(
            format!(
                "{} active repack job{}",
                seg.active_repacks,
                plural_suffix(seg.active_repacks)
            ),
            Style::default().fg(Color::Cyan),
        ),
        Span::raw("   "),
        Span::styled(
            format!(
                "I/O {} fetch / {} PUT / {} DELETE",
                seg.active_fetches, seg.active_puts, seg.active_deletes
            ),
            Style::default().fg(Color::Gray),
        ),
        Span::raw("   "),
        Span::styled(
            format!(
                "gather {} / {} budget",
                format_bytes_human(seg.repack_memory_reserved_bytes),
                format_bytes_human(seg.repack_memory_budget_bytes)
            ),
            Style::default().fg(Color::DarkGray),
        ),
    ]);
    f.render_widget(Paragraph::new(executor), rows[2]);

    f.render_widget(Paragraph::new(segment_reclaim_state_line(seg)), rows[3]);
}

fn segment_reclaim_state_line(seg: &proto::SegmentReclaimStatus) -> Line<'static> {
    if seg.checkpoint_pinned {
        return Line::from(Span::styled(
            "checkpoint held: reclamation and repacking paused until it is released",
            Style::default().fg(Color::Yellow),
        ));
    }

    if seg.awaiting_delete > 0 {
        return Line::from(Span::styled(
            format!(
                "{} segment{} pending deletion after the safety window",
                seg.awaiting_delete,
                plural_suffix(seg.awaiting_delete)
            ),
            Style::default().fg(Color::DarkGray),
        ));
    }
    Line::from(Span::styled(
        "no deletion pending",
        Style::default().fg(Color::DarkGray),
    ))
}

fn render_memory_stats(f: &mut Frame, app: &MonitorApp, area: Rect) {
    let s = app.current.as_ref();
    let allocated = s.map(|s| s.jemalloc_allocated).unwrap_or(0);
    let resident = s.map(|s| s.jemalloc_resident).unwrap_or(0);
    let retained = s.map(|s| s.jemalloc_retained).unwrap_or(0);
    let metadata = s.map(|s| s.jemalloc_metadata).unwrap_or(0);

    let fragmentation = if allocated > 0 {
        ((resident as f64 - allocated as f64) / allocated as f64 * 100.0).max(0.0)
    } else {
        0.0
    };

    let lines = vec![Line::from(vec![
        Span::styled("Allocated  ", Style::default().fg(Color::White)),
        Span::styled(
            format_bytes_human(allocated),
            Style::default().fg(Color::Green),
        ),
        Span::raw("   "),
        Span::styled("Resident   ", Style::default().fg(Color::White)),
        Span::styled(
            format_bytes_human(resident),
            Style::default().fg(if fragmentation > 50.0 {
                Color::Red
            } else if fragmentation > 25.0 {
                Color::Yellow
            } else {
                Color::Green
            }),
        ),
        Span::raw("   "),
        Span::styled("Frag  ", Style::default().fg(Color::White)),
        Span::raw(format!("{:.1}%", fragmentation)),
        Span::raw("   "),
        Span::styled("Retained  ", Style::default().fg(Color::White)),
        Span::raw(format_bytes_human(retained)),
        Span::raw("   "),
        Span::styled("Metadata  ", Style::default().fg(Color::White)),
        Span::raw(format_bytes_human(metadata)),
    ])];
    let para = Paragraph::new(lines).block(
        Block::bordered()
            .title(" jemalloc Memory ")
            .title_style(Style::default().fg(Color::Yellow).bold()),
    );
    f.render_widget(para, area);
}

pub(crate) fn format_bytes_human(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB", "EB"];
    let mut size = bytes as f64;
    for unit in UNITS {
        if size < 1024.0 {
            return format!("{:.1} {}", size, unit);
        }
        size /= 1024.0;
    }
    format!("{:.1} EB", size)
}

fn format_bytes_short(bytes: f64) -> String {
    const UNITS: &[&str] = &["B", "K", "M", "G", "T", "P", "E"];
    let mut size = bytes;
    for unit in UNITS {
        if size < 1024.0 {
            return if *unit == "B" {
                format!("{:.0}{}", size, unit)
            } else {
                format!("{:.1}{}", size, unit)
            };
        }
        size /= 1024.0;
    }
    format!("{:.1}E", size)
}

fn format_ops(ops: f64) -> String {
    if ops >= 1_000_000.0 {
        format!("{:.1}M", ops / 1_000_000.0)
    } else if ops >= 1_000.0 {
        format!("{:.1}k", ops / 1_000.0)
    } else {
        format!("{:.0}", ops)
    }
}

fn plural_suffix(count: u64) -> &'static str {
    if count == 1 { "" } else { "s" }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;

    const GIB: u64 = 1024 * 1024 * 1024;

    fn draw(seg: proto::SegmentReclaimStatus) -> Terminal<TestBackend> {
        let mut app = MonitorApp::new();
        app.current = Some(proto::StatsSnapshot {
            segment_reclaim: Some(seg),
            ..Default::default()
        });
        let mut terminal = Terminal::new(TestBackend::new(100, 6)).unwrap();
        terminal
            .draw(|f| render_segment_space(f, &app, f.area()))
            .unwrap();
        terminal
    }

    fn render(seg: proto::SegmentReclaimStatus) -> String {
        let terminal = draw(seg);
        format!("{}", terminal.backend())
    }

    /// Count the live/dead/awaiting cells in the bar row (inner y = 0, buffer
    /// y = 1, inside the top border). Live is the gray track, dead is red,
    /// awaiting-delete is yellow.
    fn bar_bands(seg: proto::SegmentReclaimStatus) -> (usize, usize, usize) {
        let terminal = draw(seg);
        let buf = terminal.backend().buffer();
        let (mut live, mut dead, mut awaiting) = (0, 0, 0);
        for x in 0..buf.area().width {
            match buf.cell((x, 1u16)).map(|c| c.fg) {
                Some(Color::Gray) => live += 1,
                Some(Color::Red) => dead += 1,
                Some(Color::Yellow) => awaiting += 1,
                _ => {}
            }
        }
        (live, dead, awaiting)
    }

    #[test]
    fn bar_bands_are_proportional() {
        // 40G live / 13G dead-in-live / 2G awaiting of 55G, over a 98-wide
        // inner area: 71/23/3 by floor, the 1-cell floor slack going to live.
        let (live, dead, awaiting) = bar_bands(proto::SegmentReclaimStatus {
            appended_bytes: 55 * GIB,
            live_bytes: 40 * GIB,
            reclaimable_bytes: 15 * GIB,
            awaiting_delete_bytes: 2 * GIB,
            ..Default::default()
        });
        assert_eq!(live + dead + awaiting, 98, "bar fills the inner width");
        assert_eq!((live, dead, awaiting), (72, 23, 3));
    }

    #[test]
    fn active_repack_resources_are_rendered_directly() {
        let out = render(proto::SegmentReclaimStatus {
            active_repacks: 3,
            active_fetches: 11,
            active_puts: 2,
            active_deletes: 4,
            repack_memory_reserved_bytes: 384 * 1024 * 1024,
            repack_memory_budget_bytes: GIB,
            ..Default::default()
        });
        assert!(out.contains("3 active repack jobs"), "{out}");
        assert!(out.contains("I/O 11 fetch / 2 PUT / 4 DELETE"), "{out}");
        assert!(out.contains("gather 384.0 MB / 1.0 GB budget"), "{out}");
    }

    // Raw buffered bytes are not necessarily live, so they are independent.
    #[test]
    fn buffered_bytes_do_not_reduce_the_live_total() {
        let out = render(proto::SegmentReclaimStatus {
            appended_bytes: 50 * GIB,
            live_bytes: 40 * GIB,
            reclaimable_bytes: 10 * GIB,
            unflushed_bytes: 512 * 1024 * 1024,
            ..Default::default()
        });
        assert!(out.contains("40.0 GB live"), "{out}");
        assert!(out.contains("512.0 MB buffered"), "{out}");
        assert!(!out.contains("durable"), "{out}");
    }
}
