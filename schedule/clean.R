library(dplyr)
library(tidyr)

# pull in data
opp_grid <- read.csv('opp_grid.csv', header=TRUE, stringsAsFactors = FALSE)
game_times_grid <- read.csv('game_times_grid.csv', header=TRUE, stringsAsFactors = FALSE)

# normalize
opp_grid_norm <- opp_grid %>%
  gather(Week, Opp, X1:X17) %>%
  mutate(Week = as.integer(substr(Week, 2, nchar(Week)))) %>%
  filter(Opp != 'BYE') %>%
  mutate(
    Home = ifelse(substr(Opp, 1, 1)=="@", substr(Opp, 2, nchar(Opp)), Team),
    Away = ifelse(substr(Opp, 1, 1)=="@", Team, Opp)
  ) %>%
  select(Home, Away, Week) %>%
  distinct()

game_times_grid_norm <- game_times_grid %>%
  gather(Week, Time, X1:X17) %>%
  mutate(Week = as.integer(substr(Week, 2, nchar(Week)))) %>%
  filter(Time != 'BYE')

# merge
games <- opp_grid_norm %>%
  inner_join(game_times_grid_norm, by = c("Home"="Team","Week"="Week"))

# de-dupe (international games don't have Home/Away)
games$Game <- apply(games[,c("Home", "Away")], 1, function(x) paste(sort(x), collapse = "|"))

games <- games %>%
  group_by(Game, Week) %>%
  arrange(Home) %>%
  slice(1) %>%
  ungroup() %>%
  select(-Game) %>%
  arrange(Week, Home)

# export
write.csv(games, 'schedule.csv', row.names = FALSE, na = "")
