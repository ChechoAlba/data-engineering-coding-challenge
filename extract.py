import xml.etree.ElementTree as ET

def define_root():
    tree = ET.parse('data\Q9Y261.xml')
    root = tree.getroot()
    return root

def write_protein(root):
    protein = [accesion.text for accesion in root.iter('{http://uniprot.org/uniprot}accession')]
    return protein

def write_name(root):
    for protein in root.iter('{http://uniprot.org/uniprot}protein'):
        names = [{'name':name[0].text,
             'type':name.tag[28:]} for name in protein]
    return names

def write_gen(root):
    for gene in root.iter('{http://uniprot.org/uniprot}gene'):
        genes = [{'name':name.text,
             'type':name.attrib['type']} for name in gene]
    return genes

def write_organism(root):
    for organism in root.iter('{http://uniprot.org/uniprot}organism'):
        for name in organism.iter('{http://uniprot.org/uniprot}name'):
            if(name.attrib['type'] == 'scientific'):
                nameOrganism = name.text
                typeOrgnaism = name.attrib['type']
                break
        for lineage in organism.iter('{http://uniprot.org/uniprot}lineage'):
            taxonList= [taxon.text for taxon in lineage]
    organism = {'name':nameOrganism,
                'type':typeOrgnaism,
                'lineage':taxonList}
    return organism

def write_reference(root):
    references = []
    for reference in root.iter('{http://uniprot.org/uniprot}reference'):
        key = reference.attrib['key']
        for citation in reference.iter('{http://uniprot.org/uniprot}citation'):
            for title in citation.iter('{http://uniprot.org/uniprot}title'):
                title = title.text
            for authorList in citation.iter('{http://uniprot.org/uniprot}authorList'): 
                authors = [author.attrib['name'] for author in authorList ]
        scopes = [scope.text for scope in reference.iter('{http://uniprot.org/uniprot}scope') ]
        references.append({'key':key,
                   'title':title,
                    'authors':authors,
                    'scopes':scopes})
    return references

def write_comments(root):
    comments = []
    for comment in root.iter('{http://uniprot.org/uniprot}comment'):
        type = comment.attrib['type']
        for text in comment.iter('{http://uniprot.org/uniprot}text'):
            text = text.text
        comments.append({'text':text,
             'type':type})
    return comments

def write_dbReference(root):
    dbReferences = [{'type': dbReference.attrib['type'],
                     'id': dbReference.attrib['id']} for dbReference in root.iter('{http://uniprot.org/uniprot}dbReference')]
    return dbReferences

def write_keyword(root):
    keywords = [{'text': keyword.text,
                     'id': keyword.attrib['id']} for keyword in root.iter('{http://uniprot.org/uniprot}keyword')]
    return keywords

def write_feature(root):
    features = [feature.attrib for feature in root.iter('{http://uniprot.org/uniprot}feature')]
    return features

def write_evidence(root):
    evidences = [evidence.attrib for evidence in root.iter('{http://uniprot.org/uniprot}evidence')]
    return evidences

def main():
    root = define_root()
    print(20*'#' + " Proteins " + 20*'#')
    write_protein(root)
    print(20*'#' + " Names " + 20*'#')
    write_name(root)
    print(20*'#' + " Genes " + 20*'#')
    write_gen(root)
    print(20*'#' + " Organisms " + 20*'#')
    print(write_organism(root))
    print(20*'#' + " References " + 20*'#')
    write_reference(root)
    print(20*'#' + " Comments " + 20*'#')
    write_comments(root)
    print(20*'#' + " DbReferences " + 20*'#')
    write_dbReference(root)
    print(20*'#' + " Keywords " + 20*'#')
    write_keyword(root)
    print(20*'#' + " features " + 20*'#')
    print(write_feature(root))
    print(20*'#' + " Evidences " + 20*'#')
    print(write_evidence(root))

if __name__ == "__main__":
    main()
