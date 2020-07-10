module DataCollection

export AbstractParameters, AbstractData, addrow!, tokw

# stdlib
using Dates
using Serialization

# "external" dependencies
using DataFrames
import DataStructures: SortedDict

#####
##### Experiment Parameters
#####

# Add a hook for modifying values when we turn Parameters structs into dictionaries
lower(x) = x
lower(f::Function) = string(f)

# Use experiment parameters to collect required information for each run.
abstract type AbstractParameters end
abstract type AbstractData end

const PType = Union{AbstractParameters, AbstractData}

kwexclude(::PType) = ()

#####
##### GenericParameters and GenericData
#####

namesof(nt::NamedTuple{names}) where {names} = names

struct GenericParameters{names,T} <: AbstractParameters
    nt::NamedTuple{names,T}
end
GenericParameters(; kw...) = GenericParameters((;kw...,))

struct GenericData{names,T} <: AbstractData
    nt::NamedTuple{names,T}
end
GenericData(; kw...) = GenericData((;kw...,))

Base.propertynames(x::Union{GenericParameters,GenericData}) = namesof(x.nt)
function Base.getproperty(x::Union{GenericParameters,GenericData}, s::Symbol)
    nt = getfield(x, :nt)
    s == :nt && return nt
    return nt[s]
end

# DataFrame hooks
dict(x::AbstractDict) = x
dict(x::PType) = SortedDict(k => lower(getproperty(x,k)) for k in propertynames(x))

dict(x...) = merge(combine_error, dict.(x)...)
combine_error(a,b) = error("Found duplicate keys! Values are `$a` and `$b`.")

# Support for turning parameters into a collection of keyword arguments
tokw(x::PType) = (k => getproperty(x,k) for k in propertynames(x) if !in(k, kwexclude(x)))

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
    push!(df, merge(timestamp(), dict(y, d)); promote = true, cols = cols)
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
    pred = kv -> hasproperty(x, first(kv)) && unmissing(getproperty(x, first(kv)) == last(kv))
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

#####
##### Handle aggregates of measurements
#####

mutable struct MeasurementBundle{T, A <: NamedTuple, B <: NamedTuple}
    measurements::T
    pre::A
    post::B
end

function MeasurementBundle(measurements; pre = NamedTuple(), post = NamedTuple())
    return MeasurementBundle(measurements, pre, post)
end

# It's important that steping each of the inner iterators happens only once every time
# we iterate on a measurement bundle in case some global monitoring is being performed
# by some of the inner objects.
function _iterate(m::MeasurementBundle, states)
    # Only stop iterating when all states are empty
    all(isnothing, states) && return nothing

    # We return a named tuple that's the collection of all non-finished measurements
    # in the `Measurements` vector.
    #
    # Assume that each sub-iterator returns a named tuple itself.
    namedtuples = Any[first(s) for s in states if !isnothing(s)]

    # Bookend with timestamps
    pushfirst!(namedtuples, m.pre)
    push!(namedtuples, m.post)

    res = reduce(merge, namedtuples)
    return res, states
end

maybeiterate(itr, state::Tuple) = iterate(itr, last(state))
maybeiterate(itr, ::Nothing) = nothing

Base.iterate(m::MeasurementBundle) = _iterate(m, iterate.(m.measurements))
Base.iterate(m::MeasurementBundle, s) = _iterate(m, maybeiterate.(m.measurements, s))

function postprocess(m::MeasurementBundle, states, args...)
    for (measurement, state) in zip(m.measurements, states)
        isnothing(state) && continue
        postprocess(measurement, args...)
    end
    return nothing
end

end # module

