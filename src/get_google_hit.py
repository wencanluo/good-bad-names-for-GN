#get good hit for all the names
import multiprocessing as mp
from google import GoogleSearchEngine

gse = GoogleSearchEngine()

def get_google_hit(name, pos, output):
    hit, url = gse.get_google_hit(name)
    output.put((pos, hit))

def get_google_hit_parallel(names):
    output = mp.Queue()
    
    processes = [mp.Process(target=get_google_hit, args=(name, i, output)) for (i, name) in enumerate(names)]
    
    for p in processes:
        p.start()
    
    for p in processes:
        p.join()
    
    results = [output.get() for p in processes]
    results.sort()
    results = [r[1] for r in results]
    
    return results
 
 
def get_google_hit_sequential(names):
    results = []
    for name in names:
        hit, url = gse.get_google_hit(name)
        results.append(hit)
    return results
        
if __name__ == "__main__":
    names = ['parallxl', 'google', 'Aa-ascidiacea', 'Aa-bryozoa', 'Aa-caridea', 'Aa-cirripedia', 'Aa-cladocera', 'Aa-copepoda', 'Aa-corophioidea']
    import time
    
    time_start = time.clock()
    results = get_google_hit_parallel(names)
    print results
    time_end = time.clock()
    print "parallel time: %s" % (time_end - time_start)
    
    time_start = time.clock()
    results = get_google_hit_sequential(names)
    print results
    time_end = time.clock()
    print "sequential time: %s" % (time_end - time_start)
    
    
	
	
    