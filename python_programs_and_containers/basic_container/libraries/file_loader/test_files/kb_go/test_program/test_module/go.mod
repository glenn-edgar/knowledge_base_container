module testmod

go 1.24.4

require testmod/greet v0.0.0

replace testmod/greet v0.0.0 => ./greet
