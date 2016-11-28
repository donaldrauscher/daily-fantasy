import luigi, datetime, re, pickle, yaml
import pandas as pd
import numpy as np
from sklearn import linear_model

# import key dates
import key_dates

# pull in meta data
with open('../meta.yaml', 'rb') as f:
    META = yaml.load(f)

# task for fetching NF projections
class GetNumberFireProjections(luigi.Task):

    dt = luigi.DateParameter()
    pos = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('%s/nf_proj_%s_%s.csv' % (META['NF_DATA_URL'], self.pos, self.dt))

    def shift(self, col, offset):
        return col[-offset:] + col[:-offset]

    def name_process(self, x):
        if self.pos == 'D':
            return self.team_process(x)
        else:
            return ' '.join(x.split(' ')[2:4])

    def team_process(self, x):
        team_raw = x.split(' D/ST ')[0]
        try:
            team = META['NF_TEAM_MAP'][team_raw]
        except KeyError:
            team = team_raw
        return team

    def get_injury(self, player):
        result = ''
        player_w = player.split(' ')
        for inj in META['INJ']:
            if inj in player_w:
                result = inj
                break
        return result

    def run(self):

        # grab data
        url = META['NF_DAILY_FANTASY_URL'] + '/' + self.pos
        dfs = pd.read_html(url, attrs={'class':'projection-table'})
        dfs = [x for x in dfs if x.shape[0] > 0]
        dfs = pd.concat(dfs, axis=1)

        # clean up column names; drop blank 2nd column
        dfs.rename(columns=lambda x: re.sub('\s+', '', x), inplace=True)
        dfs.drop(dfs.columns[1], inplace=True, axis=1)

        # fix column header shift
        col_names = list(dfs.columns.values)
        col_names2 = self.shift(col_names, -2)
        col_names2[0] = 'Player'
        dfs.rename(columns=dict(zip(col_names, col_names2)), inplace=True)
        dfs.drop(dfs.columns[-2:], inplace=True, axis=1)

        # add injury; extract player name
        dfs.insert(loc=1, column='Inj', value=dfs.Player.apply(self.get_injury))
        dfs['Player'] = dfs.Player.apply(self.name_process)

        # fix `Cost`
        dfs['Cost'] = dfs.Cost.apply(lambda x: int(re.sub('[$\,]', '', str(x))))

        # export
        dfs.to_csv(self.output().path, index=False)

# task for fetching NF confidence intervals
class GetNumberFireConfidenceIntervals(luigi.Task):

    dt = luigi.DateParameter()
    pos = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('%s/nf_ci_%s_%s.csv' % (META['NF_DATA_URL'], self.pos, self.dt))

    def name_process(self, x):
        if self.pos == 'D':
            return self.team_process(x)
        else:
            return ' '.join(x.split(' ')[0:2])

    def team_process(self, x):
        team_raw = x.split(', ')[1][:-1]
        try:
            team = META['NF_TEAM_MAP'][team_raw]
        except KeyError:
            team = team_raw
        return team

    def run(self):

        # grab data
        url = META['NF_REG_FANTASY_URL'] + '/' + self.pos.lower()
        dfs = pd.read_html(url, attrs={'class':'projection-table'})
        dfs = [x for x in dfs if x.shape[0] > 0]
        dfs = pd.concat(dfs, axis=1)

        # clean up column names; drop blank 2nd column
        dfs.rename(columns=lambda x: re.sub('\s+', '', x), inplace=True)
        dfs.drop(dfs.columns[1], inplace=True, axis=1)

        # filter to first three columns and rename
        dfs = dfs.ix[:,0:3].copy()
        dfs.rename(columns=dict(zip(list(dfs.columns.values), ['Player', 'Proj', 'CI'])), inplace=True)

        # add position; extract name / team
        dfs.insert(loc=1, column='Team', value=dfs.Player.apply(self.team_process))
        dfs['Player'] = dfs.Player.apply(self.name_process)

        # split CI and extract standard deviation
        CI2 = dfs['CI'].apply(lambda x: x[1:] if x[0] == '-' else x)
        dfs['CI_Lower'], dfs['CI_Upper'] = CI2.str.split('-', 1).str
        for c in ['Proj', 'CI_Lower', 'CI_Upper']:
            dfs[c] = pd.to_numeric(dfs[c])
        dfs['SD'] = dfs.CI_Upper - dfs.Proj
        dfs['CI_Lower'] = dfs.Proj - dfs.SD
        dfs.drop('CI', inplace=True, axis=1)

        # export
        dfs.to_csv(self.output().path, index=False)

# wrapper task for fetching all NF data
class GetAllNumberFireData(luigi.Task):

    dt = luigi.DateParameter()

    def output(self):
        return luigi.LocalTarget('%s/nf_data_%s.csv' % (META['NF_DATA_URL'], self.dt))

    def run(self):

        # grab projection data
        nf_data1 = [GetNumberFireProjections(dt=self.dt, pos=pos) for pos in META['POS']]
        yield nf_data1
        nf_data1 = [pd.read_csv(x.output().path, index_col="Player") for x in nf_data1]
        nf_data1 = [df[["Inj", "FP", "Cost"]] for df in nf_data1]
        nf_data1 = pd.concat(nf_data1, axis=0, keys=META['POS'], names=('Pos', 'Player'))

        # grab confidence interval data
        nf_data2 = [GetNumberFireConfidenceIntervals(dt=self.dt, pos=pos) for pos in META['POS']]
        yield nf_data2
        nf_data2 = [pd.read_csv(x.output().path, index_col="Player") for x in nf_data2]
        nf_data2 = [df[["Team", "Proj", "CI_Lower", "CI_Upper", "SD"]] for df in nf_data2]
        nf_data2 = pd.concat(nf_data2, axis=0, keys=META['POS'], names=('Pos', 'Player'))

        # join together
        nf_data = pd.concat([nf_data1, nf_data2], axis=1, join="inner")

        # export
        nf_data.to_csv(self.output().path)

# determine scalers from regular fantasy to daily fantasy
class Reg2DailyScalers(luigi.Task):

    dt = luigi.DateParameter()

    def output(self):
        return luigi.LocalTarget('%s/reg2daily_scalers_%s.p' % (META['NF_DATA_URL'], self.dt))

    def requires(self):
        return GetAllNumberFireData(dt=self.dt)

    def run(self):

        # get nf data
        nf_data = pd.read_csv(self.input().path)

        # scaler for each position
        scalers = {}
        for pos in META['POS']:

            # filter data
            nf_data_pos = nf_data[nf_data.Pos == pos]

            # tune model
            regr = linear_model.LinearRegression(fit_intercept=False)
            data_x = nf_data_pos.Proj.as_matrix().reshape(-1, 1)
            data_y = nf_data_pos.FP.as_matrix()
            regr.fit(X=data_x, y=data_y)

            # save coeff
            scalers[pos] = float(regr.coef_[0])

        # export
        print(scalers)
        pickle.dump(scalers, open(self.output().path, "wb"))

# apply scalers
class GetAllNumberFireDataFinal(luigi.Task):

    dt = luigi.DateParameter(default = key_dates.TUE)

    def output(self):
        return luigi.LocalTarget('%s/nf_data_%s_final.csv' % (META['NF_DATA_URL'], self.dt))

    def requires(self):
        return {
            'nf_data': GetAllNumberFireData(dt=self.dt),
            'scalers': Reg2DailyScalers(dt=self.dt)
        }

    def run(self):

        # get nf data
        nf_data = pd.read_csv(self.input()['nf_data'].path)
        scalers = pickle.load(open(self.input()['scalers'].path, "rb" ))

        # apply scalers
        nf_data['SD'] = nf_data.apply(lambda row: row.SD * scalers[row.Pos], axis=1)
        nf_data['CI_Upper'] = nf_data.FP + nf_data.SD
        nf_data['CI_Lower'] = nf_data.FP - nf_data.SD

        # clean up
        nf_data.drop("Proj", inplace=True, axis=1)
        nf_data.rename(columns={"FP":"Proj"}, inplace=True)
        nf_data = nf_data.round({'Proj':1, 'SD':1, 'CI_Upper':1, 'CI_Lower':1})

        # add opponent and game time
        nf_data['Opp'] = nf_data.Team.apply(lambda x: META['SCHEDULE_OPP'][x][key_dates.WEEK-1])
        nf_data['Time'] = nf_data.Team.apply(lambda x: META['SCHEDULE_TIMES'][x][key_dates.WEEK-1])

        # add an index
        nf_data['Index'] = [i for i in range(nf_data.shape[0])]

        # rearrange
        nf_data = nf_data[["Index", "Pos", "Player", "Team", "Opp", "Time", "Inj", "Cost", "Proj", "SD", "CI_Lower", "CI_Upper"]]

        # export
        nf_data.to_csv(self.output().path, index=False)

if __name__ == "__main__":
    luigi.run()
