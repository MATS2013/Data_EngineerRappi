import sys
def _last_slash(text):
    """Funcion que obtiene el ultimo slash de una ruta si retorna -1 es que no tiene slash"""
    n = len(text)
    s =-1
    for i in range(0,n):
        if text[i] == str(chr(92)):
            s = i
    return s
def _get_current_FilePath():    
    #Reemplaza "/" por "\""
    parent_dir = str(sys.argv[0]).replace("/", chr(92))
    #Funcion para asegurar que la ruta sea real,
    #parece redundante, pero al convertir el archivo
    #.py a .exe cobra sentido
    parent_dir = parent_dir[:_last_slash(parent_dir)+1]
    return parent_dir
current_path = _get_current_FilePath() 