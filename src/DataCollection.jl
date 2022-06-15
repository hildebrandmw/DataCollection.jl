module DataCollection

export AbstractParameters, AbstractData, Parameters, Data, addrow!, tokw

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
struct Parameters{names,T} <: AbstractParameters
    nt::NamedTuple{names,T}
end
Parameters(; kw...) = Parameters((; kw...))
const GenericParameters = Parameters

struct Data{names,T} <: AbstractData
    nt::NamedTuple{names,T}
end
Data(; kw...) = Data((; kw...))
const GenericData = Data

Base.propertynames(x::Union{Parameters,Data}) = namesof(x.nt)
function Base.getproperty(x::Union{Parameters,Data}, s::Symbol)
    nt = getfield(x, :nt)
    s == :nt && return nt
    return nt[s]
end

getnt(a::Union{Parameters,Data}) = a.nt
Base.merge(a::Parameters, b::Parameters...) = Parameters(merge(getnt(a), getnt.(b)...))
Base.merge(a::Data, b::Data...) = Data(merge(getnt(a), getnt.(b)...))

# DataFrame hooks
combine_error(a, b) = error("Found duplicate keys! Values are `$a` and `$b`.")
function dict(x::Bundle)
    return SortedDict(
        k => lower(getproperty(x, k)) for k in propertynames(x) if !in(k, paramexclude(x))
    )
end
lower(x::Bundle) = dict(x)

# Support for turning parameters into a collection of keyword arguments
function tokw(x::Bundle)
    return (k => getproperty(x, k) for k in propertynames(x) if !in(k, kwexclude(x)))
end

timestamp() = SortedDict(:timestamp => now())

_lower(x...) = merge(combine_error, map(lower, x)...)
function addrow!(
    df::DataFrame,
    y::AbstractData,
    x::Union{AbstractParameters,<:AbstractDict}...;
    cols = :setequal,
    sanitizer = Returns(nothing),
    force = false,
)
    # if the datacrame is empty, then we want to set `cols` to `:union` in order
    # to bootstrap DataFrame creation
    d = _lower(x...)
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
    newrow = _lower(timestamp(), y, d)
    sanitizer(newrow)
    return push!(df, newrow; promote = true, cols = cols)
end

function addrow!(
    savefile::AbstractString,
    y::AbstractData,
    x::Union{AbstractParameters,<:AbstractDict}...;
    kw...,
)
    df = load(savefile)
    addrow!(df, y, x...; kw...)
    save(df, savefile)
    return df
end

# Utilities for searching for existing entries.
struct RowSearch{T}
    dict::T
end

RowSearch(x::AbstractParameters...) = RowSearch(_lower(x...))

# Treat `missing` as falses.
unmissing(x::Bool) = x
unmissing(x::Missing) = false

function (R::RowSearch)(x)
    pred =
        kv -> haskey(x, first(kv)) && unmissing(getindex(x, first(kv)) == last(kv))
    return all(pred, R.dict)
end

findrow(df::DataFrame, x::AbstractParameters...) = findrow(df, _lower(x...))
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

