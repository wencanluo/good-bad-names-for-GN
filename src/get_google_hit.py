#get good hit for all the names
import multiprocessing as mp
from google import GoogleSearchEngine

gse = GoogleSearchEngine()

def single_task(task, arguemnt, pos, output):
    res = task(arguemnt)
    output.put((pos, res))

def task_parallel(task, argruments):
    output = mp.Queue()
    
    processes = [mp.Process(target=single_task, args=(task, argument, i, output)) for (i, argument) in enumerate(argruments)]
    
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
    results = task_parallel(gse.get_google_hit, names)
    print results
    time_end = time.clock()
    print "parallel time: %s" % (time_end - time_start)
    
    
	
	
    