import groovy.transform.MapConstructor
import groovy.xml.MarkupBuilder
import groovy.xml.XmlSlurper

@Grab('org.codehaus.groovy.modules.http-builder:http-builder:0.7')
import groovyx.net.http.ContentType
import groovyx.net.http.HTTPBuilder
import groovyx.net.http.HttpResponseDecorator
import groovyx.net.http.Method

@Grab('org.apache.httpcomponents:httpmime:4.5.1')
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.mime.MultipartEntityBuilder
import org.apache.http.entity.mime.content.StringBody

// https://wwwdev.ebi.ac.uk/ena/submit/webin/auth/swagger-ui/index.html?configUrl=/ena/submit/webin/auth/v3/api-docs/swagger-config#/AuthenticationAPI/getToken
// https://www.ebi.ac.uk/ena/submit/drop-box/swagger-ui/index.html?configUrl=/ena/submit/drop-box/v3/api-docs/swagger-config
//https://www.ebi.ac.uk/ena/portal/api/returnFields?result=read_run
class ENADropBoxClient {
    final String endpointURL = "www.ebi.ac.uk/ena/submit/drop-box/submit"
    final String testEndpointURL = "wwwdev.ebi.ac.uk/ena/submit/drop-box/submit"
    final HTTPBuilder http
    final String center_name


    ENADropBoxClient(webinUser, webinPass, isTest, center_name) {
        def activeEndpoint = isTest ? testEndpointURL : endpointURL
        this.http = new HTTPBuilder("https://${webinUser}:${webinPass}@${activeEndpoint}")
        this.center_name = center_name
    }

    Response addSample(Map<String, String> sample_record) {
        sample("ADD", sample_record)
    }

    Response modifySample(Map<String, String> sample_record) {
        sample("MODIFY", sample_record)
    }

    Response sample(String action, Map<String, String> sample_record) {
        submit(action, "SAMPLE", sampleRecordXml(center_name, sample_record))
    }

    Response addProject(String project_alias) {
        project("ADD", project_alias)
    }

    Response modifyProject(String project_alias) {
        project("MODIFY", project_alias)
    }

    Response project(String action, String project_alias) {
        submit(action, "PROJECT", projectRecordXml(center_name, project_alias, project_alias, project_alias))
    }

    @MapConstructor
    static class Response {
        String TYPE
        String alias
        String sra_accession
        String biosample_accession
        String center_name
        String broker_name
        Boolean successful
        List<String> errorLines
        List<String> infoLine
        String response_xml
        String request_xml
    }

    Response submit(String action, String payload_type, String xml_payload) {
        http.handler.success = { HttpResponseDecorator resp, Reader content -> parseResponse(xml_payload, content.getText()) }
        http.handler.failure = { resp -> throw new RuntimeException("ENA request failed: ${resp.statusLine}") }
        http.parser[ContentType.XML] = http.parser::parseText

        (Response) http.request(Method.POST, ContentType.XML, (HttpPost req) -> {
            MultipartEntityBuilder.create().with {
                addPart('ACTION', new StringBody(action))
                addPart(payload_type, new StringBody(xml_payload, org.apache.http.entity.ContentType.APPLICATION_XML))
                req.entity = build()
            }
        })
    }

    static Response parseResponse(String request_xml, String xml_string) {
        def xml = new XmlSlurper().parseText(xml_string)
        def studyAccession = xml['*'][0].@accession.toString()
        def hasErrors = xml.MESSAGES.ERROR.size()>0

        if(hasErrors || studyAccession == "") {
            throw new IllegalStateException(xml_string + "\n\n" + request_xml)
        }

        new Response(
                TYPE: xml.children().first().name,
                alias: xml.children().first()['@alias'],
                sra_accession: xml.children().first()['@accession'],
                biosample_accession: xml.children().first()['EXT_ID']['@accession'],
                center_name: xml['@center_name'],
                broker_name: xml['@broker_name'],
                successful: xml['success'] == 'true',
                errorLines: xml.MESSAGES.ERROR.list().collect{it->it.text},
                infoLine: xml.MESSAGES.INFO.list().collect{it->it.text},
                response_xml: xml_string,
                request_xml: request_xml
        )
    }

    static String projectRecordXml(String center_name, String alias, String title, String description) {
        def writer = new StringWriter()
        def xml = new MarkupBuilder(writer)

        xml.mkp.xmlDeclaration(version: "1.0", encoding: "UTF-8", standalone: "no")
        xml.PROJECT_SET() {
            PROJECT(center_name: center_name, alias: alias) {
                TITLE(title)
                DESCRIPTION(description)
                SUBMISSION_PROJECT() {
                    SEQUENCING_PROJECT()
                }
            }
        }
        return writer.toString()
    }

    static String sampleRecordXml(String center_name, Map<String,String> sampleRecord) {
        def writer = new StringWriter()
        def generic = sampleRecord.subMap([
                'sample_alias',
                'sample_title',
                'tax_id',
                'scientific_name',
                'sample_description'
        ])

        def checklist = sampleRecord - generic

        def xml = new MarkupBuilder(writer)
        xml.mkp.xmlDeclaration(version: "1.0", encoding: "UTF-8", standalone: "no")
        xml.SAMPLE_SET() {
            SAMPLE(center_name: center_name, alias: generic['sample_alias']) {
                TITLE(generic['sample_title'])
                SAMPLE_NAME() {
                    TAXON_ID(generic['tax_id'])
                    SCIENTIFIC_NAME(generic['scientific_name'])
                }
                DESCRIPTION(generic['sample_description'])
                SAMPLE_ATTRIBUTES() {
                    checklist.findAll {!it.key.endsWith('[unit]')}.each {
                        String key, String value->
                            SAMPLE_ATTRIBUTE() {
                                TAG(key)
                                VALUE(value)
                                if(checklist.get(key+'[unit]')) {
                                    UNITS(checklist.get(key+'[unit]'))
                                }
                            }
                    }
                }
            }
        }
        return writer.toString()
    }

}
