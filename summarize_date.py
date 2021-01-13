import pandas as pd
from fecfile.dataframe import Fecfile
import sys
from pathlib import Path

dates = sys.argv[1:]
if dates[0] == "--force":
    dates = dates[1:]
    force = True
else:
    force = False
    
def network_els(filing):
    # Break down a long filing parquet frame into just the basics; donor, recipient, date, and amount.
    # Donor identification is kind of dicey.
    frame = pd.read_parquet(filing)
    p = Path(filing)
    _, date, form, id = p.with_suffix("").parts

    if not isinstance(frame.index, pd.RangeIndex):
        # A bugfix against a previous version--this code sucks.
        redone = Fecfile(Path("fec") / date / (id + ".fec"))
        redone.prepare_itemization_buffers()
        reformed = redone.to_pandas(form)
        reformed.to_parquet(p)
        frame = reformed
    try:
        frame.loc[:,'zip'] = frame.contributor_zip_code.str.slice(0, 5)
        donor = frame[['contributor_first_name','contributor_last_name','contributor_organization_name','zip']]
        donor = donor.fillna("")
        donor = "I_" + donor.contributor_first_name + "_" + donor.contributor_last_name.replace("'", "") + "_" + donor.zip + "_" + donor.contributor_organization_name
        donor[pd.notna(frame.donor_committee_fec_id)] = frame.donor_committee_fec_id
        recipient = frame.filer_committee_id_number
        amount = frame.contribution_amount
        date = frame.contribution_date
    except AttributeError:
        if "beneficiary_candidate_fec_id" in frame.columns:
            donor = frame.filer_committee_id_number
            recipient = frame.beneficiary_candidate_fec_id.copy()
            amount = frame.expenditure_amount
            date = frame.expenditure_date
            recipient.loc[pd.isna(recipient)] = frame.beneficiary_committee_fec_id[pd.isna(recipient)]
            recipient.loc[pd.isna(recipient)] = frame.payee_organization_name[pd.isna(recipient)]
        else:
            return pd.DataFrame()
    result = pd.DataFrame({'date': date, 'recipient': recipient, 'amount': amount, "donor": donor.str.upper(), 'form': form})
    return result

def summarize(cache_file):
    cache_file = Path(cache_file)
    date = cache_file.name
    short = Path(f"totals/{date}.parquet")
    individual = Path(f"totals/{date}_long.parquet")
    if short.exists() and not force:
        return
    fs = []
    for p in Path(cache_file).glob("*/*.parquet"):
        data = network_els(p)
        
        if len(data.columns) < 4:
            continue
        data['amount'] = pd.to_numeric(data.amount, errors = "coerce")
        fs.append(data[pd.notna(data.amount)])
    if len(fs) == 0:
        return
    tots = pd.concat(fs).reset_index()
    tots.to_parquet(individual, index = False)
    tots = tots.groupby(["donor", "recipient"])['amount'].sum().reset_index()
    tots.to_parquet(short, index = False)
    
for date in dates:
    summarize(date)