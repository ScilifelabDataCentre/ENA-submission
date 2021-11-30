import groovy.json.JsonGenerator
import groovy.transform.MapConstructor

import java.nio.file.Files
import java.nio.file.Path
import java.util.zip.GZIPOutputStream

class ENAWebinCLIRunner {
    final String WEBIN_USER
    final String WEBIN_PASS
    final String center_name
    final Path jarPath
    final boolean isTest

    ENAWebinCLIRunner(String WEBIN_USER, String WEBIN_PASS, String center_name, Path jarPath, boolean isTest) {
        this.WEBIN_USER = WEBIN_USER
        this.WEBIN_PASS = WEBIN_PASS
        this.center_name = center_name
        this.jarPath = jarPath
        this.isTest = isTest
    }

    @MapConstructor
    static class Result {
        ENADropBoxClient.Response response
        String webin_report
    }

    Result addGenome(Map<String, String> genomeRecord, Path work_dir) {
        Path manifest = work_dir.resolve('genome.manifest.txt')
        Path chrlist = work_dir.resolve('genome.chromosomelist.txt.gz')
        Path fasta = work_dir.resolve('genome.fasta.gz')
        genomeRecordFiles(genomeRecord, manifest, chrlist, fasta)
        run(genomeRecord.name, manifest, work_dir)
    }


    Result run(Path manifest, Path input_dir, Path output_dir) {

        // https://ena-docs.readthedocs.io/en/latest/submit/general-guide/webin-cli.html
        String[] cmd = ([
                "java", "-jar", "${this.jarPath.toAbsolutePath()}",
                "-userName=${WEBIN_USER}", "-passwordEnv=WEBIN_PASS",
                "-centerName=${center_name}",
                "-manifest=${manifest.toAbsolutePath()}",
                "-inputDir=${input_dir.toAbsolutePath()}",
                "-outputDir=${output_dir.toAbsolutePath()}",
                "-context=genome", "-validate", "-submit"]
                + (isTest?["-test",]:[])
        )

        def out = new StringBuffer()
        def err = new StringBuffer()

        def p = new ProcessBuilder()
                .command(cmd)
                .directory(outputDir.toAbsolutePath().toFile())
                .each { it.environment().put("WEBIN_PASS", WEBIN_PASS) }
                .start()
                .each { it.consumeProcessOutput( out, err ) }
                .tap { it.waitFor() }

        if(p.exitValue()) {
            throw new IllegalStateException(outputDir.resolve("webin-cli.report").text)
        }

        def outputItem = outputDir.resolve("genome/${record_name}/")
//
//        Files.copy(
//            outputItem.resolve("submit/analysis.xml"),
//            outputDir.resolve("analysis.xml")
//        )
//        Files.copy(
//            outputItem.resolve("submit/receipt.xml"),
//            outputDir.resolve("receipt.xml")
//        )
//
        String xml_string = outputDir.resolve("receipt.xml").text

        new Result([
            response: ENADropBoxClient.parseResponse(
                    outputItem.resolve("submit/analysis.xml").getText(),
                    outputItem.resolve("submit/receipt.xml").getText()
            ),
            webin_report: outputDir.resolve("webin-cli.report").getText()
        ])

    }

    static void genomeRecordFiles( Map<String, String> genomeRecord,
                            Path genomeManifest, Path chrFile, Path fastaFile ) {
        String genomeSequenceString = genomeRecord.sequence

        def generator = new JsonGenerator.Options()
                .disableUnicodeEscaping()
                .excludeNulls()
                .build()

        fastaFile.withOutputStream {new GZIPOutputStream(it)}
                .withWriter{ it.write("${record_name} 1 Monopartite") }

        fastaFile.withOutputStream {new GZIPOutputStream(it)}
                .withWriter{ it.write(">${record_name}\n${genomeSequenceString}") }

        genomeManifest.withWriter {it.write(generator.toJson(
                genomeRecord + [
                        FASTA: fastaFile.getFileName().toString(),
                        CHROMOSOME_LIST: chrFile.getFileName().toString()
                ]
        ))}
    }
}
