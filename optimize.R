library(lpSolveAPI)
library(yaml)

# arguments:
# --obj=exp,var							...   character; whether objective function is point expectation or variance
#	--obj-dir=min,max					...		character; whether objective is to minimize or maximize
# --proj-percentile=0.7			...   numeric; percentile of output to optimize
# --constraint='exp>=120'		...   character; constraint on either expectation or variance
# --force-player='<NAME>'		...   character; force specific player into solution

# get args
args <- commandArgs(TRUE)

parse_args <- function(x){
	temp <- unlist(strsplit(sub("^--", "", x), "="))
	return(c(temp[1], paste(temp[-1], collapse='=')))
}

if (length(args) > 0){
	args2 <- as.data.frame(do.call("rbind", lapply(args, parse_args)))
	args3 <- as.list(as.character(args2[,2]))
	names(args3) <- args2[,1]
	args <- args3
} else {
	args <- list()
}

# fill with defaults if missing
if (is.null(args$obj)){
	args$obj <- 'exp'
}
if (is.null(args$`obj-dir`)){
	args$`obj-dir` <- ifelse(args$obj == 'exp', 'max', 'min')
}
if (is.null(args$`proj-percentile`)){
	args$`proj-percentile` <- 0.5
}

# extract constraints
parse_constraint <- function(x){
	op <- regexpr('[<=>]{1,}', x)
	op <- substr(x, op, op+attr(op, 'match.length')-1)
	sides <- unlist(strsplit(x, op))
	return(list('left'=sides[1], 'op'=op, 'right'=sides[2]))
}

constraints <- args[which(names(args) == 'constraint')]
constraints <- lapply(constraints, parse_constraint)

# players to force into solution
forced_players <- as.vector(args[which(names(args) == 'force-player')])

# inputs
players	<- read.csv('./data/players.csv', header=TRUE, stringsAsFactors = FALSE)
meta <- yaml.load_file('meta.yaml')

# make expectation and variance
exp <- qnorm(as.numeric(args$`proj-percentile`), mean=players$Proj, sd=players$SD)
var <- players$SD^2

# construct integer program
ip <- make.lp(nrow = 0, ncol = nrow(players))

for (p in meta$POS){
	add.constraint(ip, xt = ifelse(players$Pos == p, 1, 0), type = "=", rhs = meta$LINEUP[[p]])
}

for (c in constraints){
	add.constraint(ip, xt = get(c$left), type = c$op, rhs = as.numeric(c$right))
}

for (p in forced_players){
	add.constraint(ip, xt = ifelse(players$Player == p, 1, 0), type = "=", rhs = 1)
}

add.constraint(ip, xt = players$Cost, type = "<=", rhs = meta$BUDGET)

set.objfn(ip, obj = get(args$obj))
set.type(ip, columns = 1:nrow(players), type = c("binary"))
lp.control(ip, sense = args$`obj-dir`)

# solve
solve(ip)

# need to catch if don't have a solution...

soln <- tail(get.primal.solution(ip), nrow(players))
print('Solution:')
print(players[soln == 1,])
print(paste0('Expectation: ', round(sum(players$Proj[soln == 1]),1)))
print(paste0('Standard Deviation: ', round(sqrt(sum((players$SD[soln == 1])^2)),1)))
