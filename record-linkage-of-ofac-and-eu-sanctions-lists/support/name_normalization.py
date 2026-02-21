import re
import icu
import unicodedata

###############################
#         Parameters          #
###############################

LEGAL_TERMS = [

    # ===== Long forms =====

    # 🇲🇽 Mexico
    r"sociedad anonima promotora de inversion de capital variable",
    r"sociedad de responsabilidad limitada de capital variable",
    r"sociedad anonima de capital variable",

    # EU in general (Spanish, French, Romanian, Portuguese, Italian etc)
    r"sociedad limitada por acciones abierta",
    r"societe par actions ouverte",
    r"societatea publica pe actiuni",
    r"sociedade por acoes aberta",
    r"societa per azioni aperta",
    r"sociedad de responsabilidad limitada",    
    r"societa a responsabilita limitata",
    r"sociedad anonima",
    r"societa per azioni",    
    r"societatea pe actiuni",
    r"societe par actions",
    r"sociedad limitada",
    r"sociedade por acoes",
    r"societe a responsabilite limitee",
    r"societatea cu raspundere limitata",
    r"sociedade limitada",
    r"societa a responsabilita limitata",
    r"sociedad en comandita por acciones",
    r"societe en commandite par actions",
    r"societate in comandita pe actiuni",
    r"sociedade em comandita por acoes",
    r"societa in accomandita per azioni",
    r"sociedad en comandita simple",
    r"societe en commandite simple",
    r"societate in comandita simpla",
    r"sociedade em comandita simples",
    r"societa in accomandita semplice",
    r"akciova spolecnost",
    r"federalny statny podnik",
    r"empresa estatal federal",

    # 🇩🇪 Germany
    r"gesellschaft mit beschr[aä]nkter haftung",
    r"unternehmergesellschaft haftungsbeschrankt",

    # 🇬🇧 🇺🇸
    r"limited liability company",
    r"public limited company",
    r"private limited company",
    r"limited liability partnership",
    r"joint stock commercial bank",
    r"as a private joint stock company",
    r"open joint stock company",    
    r"private joint stock company",
    r"public joint stock company",
    r"closed joint stock company",
    r"federal state enterprise",
    r"federal state institution",
    r"joint stock company",    
    r"joint stock bank",
    r"joint stock holding",
    r"autonomous non commercial organisation",
    r"federal state governmental institution",
    r"charity association",
    r"limited",
    r"incorporated",
    r"corporation",
    r"company",

    # 🇷🇺 Russia (trasnsliterated)
    r"obshchestvo s ogranichennoy otvetstvennostyu",
    r"obshchestvo s ogranichennoi otvetstvennostyu",
    r"obshchestvo s ogranichennoj otvetstvennostyu",    
    r"publichnoe aktsionernoe obshchestvo",
    r"publichnoe aktsionernoe obschestvo",    
    r"aktsionernoe obshchestvo aktsionerny",
    r"aktsionernoe obshchestvo aktsionernoe",    
    r"aktsionernoe obshchestvo", 
    r"aktsionernoye obshchestvo",
    r"aktsionernoe obshchestvo",
    r"aktsionerny kommercheski bank",
    r"otkrytoe aktsionernoe obschchestvo",
    r"otkrytoe aktsionernoe obshchestvo aktsionerny",
    r"otkrytoe aktsionernoe obshchestvo",    
    r"federalny statny predpriyatiye",    

    # 🇮🇷 Iran
    r"sherkat sahami khass",
    r"sherkat sahami omumi",
    r"sherkat ba masouliyat mahdood"
    r"sherkat sahami",
    r"shakad sanat",
    r"shakad sanati",

    # 🇵🇰 Pakistan
    r"private limited",

    # 🇦🇪 United Arab Emirates
    r"free zone establishment",
    r"free zone company",

    # 🇨🇳 China
    r"limited company",
    r"group company",    

    # ===== Short forms =====
    
    # SA
    r"\bs\s+a\b", r"\bsa\b",
    # SAPI
    r"\bs\s+a\s+p\s+i\b", r"\bsapi\b",
    # S de RL
    r"\bs\s+de\s+r\s+l\b", r"\bs de rl\b",
    # S de RL de CV
    r"\bs\s+de\s+r\s+l\s+de\s+c\s+v\b", r"\bs de rl de cv\b",
    # SA de CV
    r"\bs\s+a\s+de\s+c\s+v\b", r"\bsa de cv\b",
    # SRL
    r"\bs\s+r\s+l\b", r"\bsrl\b",
    # SPA
    r"\bs\s+p\s+a\b", r"\bspa\b",
    # SL
    r"\bs\s+l\b", r"\bsl\b",
    # GmbH
    r"\bg\s+m\s+b\s+h\b", r"\bgmbh\b",
    # UG
    r"\bu\s+g\b", r"\bug\b",
    # MBH
    r"\bm\s+b\s+h\b", r"\bmbh\b",
    # AG
    r"\ba\s+g\b", r"\bag\b",
    # KG
    r"\bk\s+g\b", r"\bkg\b",
    # LLC
    r"\bl\s+l\s+c\b", r"\bllc\b",
    # LLP
    r"\bl\s+l\s+p\b", r"\bllp\b",
    # LTD
    r"\bl\s+t\s+d\b", r"\bltd\b",
    # PLC
    r"\bp\s+l\s+c\b", r"\bplc\b",
    # INC
    r"\bi\s+n\s+c\b", r"\binc\b",
    # CORP
    r"\bc\s+o\s+r\s+p\b", r"\bcorp\b",
    # CO
    r"\bc\s+o\b", r"\bco\b",
    # OOO
    r"\bo\s+o\s+o\b", r"\booo\b",
    # ZAO
    r"\bz\s+a\s+o\b", r"\bzao\b",
    # PAO
    r"\bp\s+a\s+o\b", r"\bpao\b",
    # AO
    r"\ba\s+o\b", r"\bao\b",
    # JSC
    r"\bj\s+s\s+c\b", r"\bjsc\b",
    # PJSC
    r"\bp\s+j\s+s\s+c\b", r"\bpjsc\b",
    # CJSC
    r"\bc\s+j\s+s\s+c\b", r"\bcjsc\b",
    # JSCB
    r"\bj\s+s\s+c\s+b\b", r"\bjscb\b",
    # NPP
    r"\bn\s+p\s+p\b", r"\bnpp\b",    
    # LDA
    r"\bl\s+d\s+a\b", r"\blda\b",
    # FZE
    r"\bf\s+z\s+e\b", r"\bfze\b",
    # FZC
    r"\bf\s+z\s+c\b", r"\bfzc\b",
    # OAO
    r"\bo\s+a\s+o\b", r"\boao\b",
    # OJSC
    r"\bo\s+j\s+s\s+c\b", r"\bojsc\b",
    # FGUP
    r"\bf\s+g\s+u\s+p\b", r"\bfgup\b",    
    # FSUE
    r"\bf\s+s\s+u\s+e\b", r"\bfsue\b",
    # JSB
    r"\bj\s+s\s+b\b", r"\bjsb\b",
    # NPO
    r"\bn\s+p\s+o\b", r"\bnpo\b",
    # NPO OF
    r"\bn\s+p\s+o\s+o\s+f\b", r"\bnpo of\b",
    # VPK
    r"\bv\s+p\s+k\b", r"\bvpk\b",
    # ZAO
    r"\bz\s+a\s+o\b", r"\bzao\b",
    # FSE
    r"\bf\s+s\s+e\b", r"\bfse\b",
    # ANO
    r"\ba\s+n\s+o\b", r"\bano\b",
    # SH
    r"\bs\s+h\b", r"\bsh\b",
    # SS
    r"\bs\s+s\b", r"\bss\b",
    # SHK
    r"\bs\s+h\s+k\b", r"\bshk\b",
    # SHS
    r"\bs\s+h\s+s\b", r"\bshs\b",
    # SHO
    r"\bs\s+h\s+o\b", r"\bsho\b",
    # SHOM
    r"\bs\s+h\s+o\s+m\b", r"\bshom\b"
]

LEGAL_REGEX = re.compile("|".join(LEGAL_TERMS), re.IGNORECASE)

###############################
#    Classes and functions    #
###############################

trans = icu.Transliterator.createInstance("Any-Latin; Latin-ASCII")

def normalize_name(text):

    if not isinstance(text, str):
        return ""

    # 1️⃣ Latin transliteration
    text = trans.transliterate(text)

    # 2️⃣ Unicode normalization
    text = unicodedata.normalize("NFKD", text)

    # 3️⃣ ASCII transformation
    text = text.encode("ascii", "ignore").decode("ascii")

    # 4️⃣ Lower case
    text = text.lower()

    # 5️⃣ Remove not [a-z] and not [0-9] characters 
    # (i.e., replace non-alphanumeric characters with spaces)
    text = re.sub(r"[^a-z0-9]", " ", text)

    # 6️⃣ Multiple spaces to single space & trim
    text = re.sub(r"\s+", " ", text).strip()

    return text

def remove_legal_forms(name: str) -> str:
    if not isinstance(name, str):
        return name
    name = LEGAL_REGEX.sub(" ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name