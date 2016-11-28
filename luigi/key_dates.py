import datetime, yaml

# pull in meta data
with open('../meta.yaml', 'rb') as f:
    META = yaml.load(f)

# set today
TODAY = datetime.date.today()
TODAY_DOW = int(TODAY.strftime('%w')) # 0 = Sunday, 1 = Monday, etc.

# set tuesday
if TODAY_DOW == 0:
    TUE = TODAY - datetime.timedelta(days=5)
elif TODAY_DOW == 1:
    TUE = TODAY - datetime.timedelta(days=6)
else:
    TUE = TODAY - datetime.timedelta(days=(TODAY_DOW-2))

# set thursday, sunday, and monday based on tuesday
THU = TUE + datetime.timedelta(days=2)
SUN = TUE + datetime.timedelta(days=5)
MON = TUE + datetime.timedelta(days=6)

# get week number
WEEK = int((TUE - META['SEASON_START']).days/7)+1
