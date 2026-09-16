#!/usr/bin/env python3
"""Reproduce the historical TWB structure check against Tableau's 2026.1 XSD.

Usage: python scripts/validate_tableau_schema.py WORKBOOK.twb OFFICIAL_SCHEMA.xsd
Requires the xmllint executable (libxml2). Does not download or modify inputs.
Tableau's 2026.1 XSD imports two namespaces without providing their definitions.
The temporary shim resolves those imports; workbooks using attributes from
either auxiliary namespace are rejected rather than weakly validated.

This is not a Desktop compatibility or visual acceptance check. The original
26.1/ManifestByVersion workbook passed this XSD check but failed native opening
in Tableau Desktop 2026.2.2 on 2026-09-15. The corrected generator uses document
format 18.1 with explicit feature flags, separately verified in Tableau. See
docs/dashboard-validation.md for native evidence and the acceptance procedure.
"""

import argparse
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

USER_NAMESPACE = "http://www.tableausoftware.com/xml/user"
XML_NAMESPACE = "http://www.w3.org/XML/1998/namespace"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workbook", type=Path)
    parser.add_argument("schema", type=Path)
    args = parser.parse_args()
    print(
        "Historical XSD structure check only: a pass does not establish Tableau "
        "Desktop compatibility or native visual correctness. See docs/dashboard-validation.md.",
        file=sys.stderr,
    )
    executable = shutil.which("xmllint")
    if not executable:
        parser.error("xmllint is required for this optional structural check")
    root = ET.parse(args.workbook).getroot()
    for node in root.iter():
        for attribute in node.attrib:
            if attribute.startswith((f"{{{USER_NAMESPACE}}}", f"{{{XML_NAMESPACE}}}")):
                parser.error("This import shim does not support workbooks using user:* or xml:* attributes")
    schema = args.schema.read_text(encoding="utf-8")
    with tempfile.TemporaryDirectory(prefix="tableau-schema-") as temp:
        directory = Path(temp)
        imports = (
            (USER_NAMESPACE, "user.xsd", '<xs:attributeGroup name="UserAttributes-AG"/>'),
            (XML_NAMESPACE, "xml.xsd", '<xs:attribute name="base" type="xs:anyURI"/>'),
        )
        for namespace, filename, body in imports:
            original = f'<xs:import namespace="{namespace}"/>'
            if schema.count(original) != 1:
                parser.error(f"Expected exactly one location-free import of {namespace}; use the official 2026.1 schema")
            schema = schema.replace(original, f'<xs:import namespace="{namespace}" schemaLocation="{filename}"/>')
            (directory / filename).write_text(
                '<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" '
                f'targetNamespace="{namespace}">{body}</xs:schema>', encoding="utf-8",
            )
        resolved = directory / "resolved.xsd"
        resolved.write_text(schema, encoding="utf-8")
        result = subprocess.run([executable, "--nonet", "--noout", "--schema", str(resolved), str(args.workbook.resolve())], check=False)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
