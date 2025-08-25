from inoutlists import load, LoaderUNXML

UN_sanctions_URL = "https://scsanctions.un.org/resources/xml/en/consolidated.xml"
UN_sanctions = load(UN_sanctions_URL, loader=LoaderUNXML, description="UN sanctions list")