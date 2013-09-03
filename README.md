SnomedCTExpressionRepository
============================

A config.xml file needs to be added to /SnomedCTExpressionRepository/src/test/resources. This file contains database login information and path to SNOMED CT OWL file. An example is given below:

<?xml version="1.0" encoding="UTF-8"?>
<configuration>
	<database>
		<url>jdbc:postgresql://127.0.0.1/termbind</url>
		<username>termbinduser</username>
		<password>the_password</password>
	</database>
	<owlapi>
		<url>file:///path/to/snomed.owl</url>
	</owlapi>
</configuration>
