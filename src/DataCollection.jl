module DataCollection

export AbstractParameters, AbstractData, addrow!, tokw

# stdlib
using Dates
using Serialization

# "external" dependencies
import DataFrames: DataFrame
import DataStructures: SortedDict

#####
##### Experiment Parameters
#####

# Add a hook for modifying values when we turn Parameters structs into dictionaries
lower(x) = x
lower(f::Function) = string(f)
lower(x::AbstractArray) = map(lower, x)

# Use experiment parameters to collect required information for each run.
abstract type AbstractParameters end
abstract type AbstractData end

const Bundle = Union{AbstractParameters,AbstractData}
kwexclude(::Bundle) = ()
paramexclude(x::Bundle) = kwexclude(x)

#####
##### GenericParameters and GenericData
#####

namesof(::NamedTuple{names}) where {names} = names

struct GenericParameters{names,T} <: AbstractParameters
    nt::NamedTuple{names,T}
end
GenericParameters(; kw...) = GenericParameters((; kw...))

struct GenericData{names,T} <: AbstractData
    nt::NamedTuple{names,T}
end
GenericData(; kw...) = GenericData((; kw...))

Base.propertynames(x::Union{GenericParameters,GenericData}) = namesof(x.nt)
function Base.getproperty(x::Union{GenericParameters,GenericData}, s::Symbol)
    nt = getfield(x, :nt)
    s == :nt && return nt
    return nt[s]
end

# DataFrame hooks
combine_error(a, b) = error("Found duplicate keys! Values are `$a` and `$b`.")
dict(x::AbstractDict) = x
function dict(x::Bundle)
    return SortedDict(
        k => lower(getproperty(x, k)) for k in propertynames(x) if !in(k, paramexclude(x))
    )
end
dict(x...) = merge(combine_error, map(dict, x)...)
lower(x::Bundle) = dict(x)

# Support for turning parameters into a collection of keyword arguments
function tokw(x::Bundle)
    return (k => getproperty(x, k) for k in propertynames(x) if !in(k, kwexclude(x)))
end

timestamp() = SortedDict(:timestamp => now())
function addrow!(
    df::DataFrame,
    y::AbstractData,
    x::Union{AbstractParameters,<:AbstractDict}...;
    cols = :setequal,
    force = false,
)

    # if the datacrame is empty, then we want to set `cols` to `:union` in order
    # to bootstrap DataFrame creation
    d = dict(x...)
    row = findrow(df, d)

    # If a row for these parameters already exists, check if we're forcing update.
    # If so, delete the row and continue.
    #
    # Otherwise, we need to throw an error
    if !isnothing(row)
        if force
            delete!(df, row)
        else
            error("Row for parameters already exists!")
        end
    end

    cols = isempty(df) ? :union : cols
    return push!(df, merge(timestamp(), dict(y, d)); promote = true, cols = cols)
end

# Utilities for searching for existing entries.
struct RowSearch{T}
    dict::T
end

RowSearch(x::AbstractParameters...) = RowSearch(dict(x...))

# Treat `missing` as falses.
unmissing(x::Bool) = x
unmissing(x::Missing) = false

function (R::RowSearch)(x)
    pred =
        kv -> hasproperty(x, first(kv)) && unmissing(getproperty(x, first(kv)) == last(kv))
    return all(pred, R.dict)
end

findrow(df::DataFrame, x::AbstractParameters...) = findrow(df, dict(x...))
findrow(df::DataFrame, x::AbstractDict) = findrow(df, RowSearch(x))
function findrow(df::DataFrame, f::RowSearch)
    for (i, row) in enumerate(eachrow(df))
        f(row) && (return i)
    end
    return nothing
end

hasrow(df::DataFrame, x...) = findrow(df, x...) !== nothing

# Do a trick of creating a temp directory and only replacing the original when done.
# This is something I learned a while back.
#
# Sometimes, serialization can error due to interrupt or for some other reason.
#
# If that happens and we're serializing to the same file we deserialized from, that file
# become corrupt and we lose data - which is really sad.
function save(df::DataFrame, path)
    temp = tempname()
    serialize(temp, df)
    mv(temp, path; force = true)
    return nothing
end

load(path::AbstractString) = ispath(path) ? deserialize(path)::DataFrame : DataFrame()

end # module

