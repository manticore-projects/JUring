package bench

import org.gradle.api.Project
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.Property

import javax.inject.Inject

/**
 * Configuration DSL for the {@code jmhReport} task.
 *
 * <pre>
 * jmhReport {
 *     jsonFile = layout.buildDirectory.file('reports/jmh/results.json')
 *     htmlFile = layout.buildDirectory.file('reports/jmh/benchmark-report.html')
 *     title    = 'My Benchmark Suite'
 * }
 * </pre>
 */
abstract class JmhReportExtension {

    /** Path to the JMH results JSON file (input). */
    abstract RegularFileProperty getJsonFile()

    /** Path to the generated HTML report (output). */
    abstract RegularFileProperty getHtmlFile()

    /** Title shown in the report header. */
    abstract Property<String> getTitle()

    @Inject
    JmhReportExtension(Project project) {
        jsonFile.convention(project.layout.buildDirectory.file('reports/jmh/results.json'))
        htmlFile.convention(project.layout.buildDirectory.file('reports/jmh/benchmark-report.html'))
        title.convention('JUring vs Standard I/O')
    }
}
