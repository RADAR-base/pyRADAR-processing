import mne
import sys

def parse_annotations(edf):
    """Returns events from edf header
    Parameters
    __________
    edf:    mne.io.edf.edf.RawEDF
        An edf file opened with the mne package
    Returns
    _______
    anno:   list
        A list of event annotations. Each row is in the format:
        [onset realtime (float),
         onset since start(float),
         duration (float),
         description (string)]
    start:  int
        The start of the measurement date/time as a unix timestamp.
    """
    start = edf.info['meas_date']
    anno = edf.find_edf_events()
    for row in anno:
        row.insert(0, start + row[0])
    return (anno, start)

def parse_all_edf(edf_files):
    """ Returns annotations from a list of edf file paths
    Parameters
    __________
    edf_files:  list
        A list of file paths to edf files.
    Returns
    _______
    anno:   list
        A list of event annotations. Each row is in the format:
        [onset realtime (float),
         onset since start(float),
         duration (float),
         description (string)]
    start:  int
        The start of the measurement date/time as a unix timestamp.
    """
    study_start = float('inf')
    anno = []
    for f in edf_files:
        try:
            edf = mne.io.read_raw_edf(f, preload=True, verbose=None)
        except ValueError:
            print('Bad channel in "{}"'.format(f),
                  'the file will not be included',
                 file=sys.stderr)
            continue
        fanno, fstart = parse_annotations(edf)
        anno.extend(fanno)
        study_start = min(study_start, fstart)

    for row in anno:
        row[1] = row[0]-study_start
    anno.sort(key=lambda x:x[0])

    return (anno, study_start)


if __name__ == '__main__':
    import argparse
    import glob
    import radar.io.csv
    parser = argparse.ArgumentParser(description=('Extract annotations from',
                                                  'edf files'))
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-f', '--file')
    group.add_argument('-d', '--dir')
    parser.add_argument('-r', '--recursive', default=False, action='store_true')
    parser.add_argument('-o', '--output', default='')
    args = parser.parse_args()

    if args.dir:
        if args.recursive:
            edf_files = glob.glob(args.dir + '**/*.edf', recursive=True)
        else:
            edf_files = glob.glob(args.dir + '*.edf')
    else:
        edf_files = [args.file]

    anno, _ = parse_all_edf(edf_files)
    radar.io.csv.write_csv(anno, fname=args.output,
                           fieldnames=['onset real time (s)',
                                       'onset study time (s)',
                                       'duration (s)',
                                       'description'])
