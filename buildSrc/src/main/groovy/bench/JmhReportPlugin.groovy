package bench

import org.gradle.api.Plugin
import org.gradle.api.Project

/**
 * Registers the {@code jmhReport} task that turns JMH results.json into
 * an interactive HTML dashboard.
 *
 * <p>Usage in {@code build.gradle}:
 * <pre>
 * plugins {
 *     id 'bench.jmh-report'
 * }
 *
 * jmhReport {
 *     jsonFile = layout.buildDirectory.file('reports/jmh/results.json')
 *     htmlFile = layout.buildDirectory.file('reports/jmh/benchmark-report.html')
 *     title    = 'JUring vs Standard I/O'
 * }
 * </pre>
 */
class JmhReportPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {

        // ── DSL extension ────────────────────────────────────────────
        def ext = project.extensions.create('jmhReport', JmhReportExtension, project)

        // ── Task ─────────────────────────────────────────────────────
        project.tasks.register('jmhReport', JmhReportTask) { task ->
            task.group       = 'reporting'
            task.description = 'Generates an interactive HTML report from JMH benchmark results'

            task.jsonFile.convention(ext.jsonFile)
            task.htmlFile.convention(ext.htmlFile)
            task.title.convention(ext.title)
        }

        // ── Auto-wire: jmhReport.dependsOn(jmh) if the task exists ──
        project.afterEvaluate {
            def jmhTask = project.tasks.findByName('jmh')
            if (jmhTask != null) {
                project.tasks.named('jmhReport').configure {
                    it.dependsOn(jmhTask)
                }
            }
        }
    }
}
